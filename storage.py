from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import aiosqlite

from max_client import split_for_max


@dataclass(frozen=True, slots=True)
class OutboxItem:
    id: int
    event_id: int
    part_index: int
    message_text: str
    status: str
    attempt_count: int
    next_attempt_at: float
    attempt_started_at: float | None


class Storage:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def open(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        connection = await aiosqlite.connect(self._path)
        connection.row_factory = aiosqlite.Row
        await connection.execute("PRAGMA journal_mode=WAL")
        await connection.execute("PRAGMA synchronous=FULL")
        await connection.execute("PRAGMA foreign_keys=ON")
        await connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS smartshell_events (
                event_id INTEGER PRIMARY KEY,
                created_at TEXT NOT NULL,
                event_type TEXT NOT NULL,
                description TEXT NOT NULL,
                status TEXT NOT NULL
                    CHECK (status IN ('queued', 'sent', 'skipped')),
                inserted_at REAL NOT NULL,
                sent_at REAL
            );

            CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL
                    REFERENCES smartshell_events(event_id) ON DELETE CASCADE,
                part_index INTEGER NOT NULL,
                message_text TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'sending', 'uncertain', 'sent', 'failed')),
                attempt_count INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL DEFAULT 0,
                attempt_started_at REAL,
                max_message_id TEXT,
                last_error TEXT,
                UNIQUE (event_id, part_index)
            );

            CREATE INDEX IF NOT EXISTS idx_outbox_status_id ON outbox(status, id);
            CREATE INDEX IF NOT EXISTS idx_outbox_status_next_attempt ON outbox(status, next_attempt_at);
            """
        )
        await connection.commit()
        self._connection = connection

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def recover_after_restart(self) -> None:
        connection = self._require_connection()
        async with self._lock:
            await connection.execute(
                """
                UPDATE outbox
                SET status = 'uncertain',
                    next_attempt_at = 0,
                    last_error = 'Process stopped while MAX send result was unknown'
                WHERE status = 'sending'
                """
            )
            await connection.commit()

    async def discard_unsent_events_before(
        self,
        event_type_prefix: str,
        before: datetime,
        reason: str,
    ) -> int:
        connection = self._require_connection()
        async with self._lock:
            await connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = await connection.execute(
                    """
                    SELECT event_id
                    FROM smartshell_events
                    WHERE created_at < ?
                      AND event_type LIKE ?
                      AND status != 'sent'
                    """,
                    (_format_dt(before), f"{event_type_prefix}%"),
                )
                event_ids = [int(row[0]) for row in await cursor.fetchall()]
                if not event_ids:
                    await connection.rollback()
                    return 0

                placeholders = ",".join("?" for _ in event_ids)
                await connection.execute(
                    f"DELETE FROM outbox WHERE event_id IN ({placeholders})",
                    tuple(event_ids),
                )
                await connection.execute(
                    f"""
                    UPDATE smartshell_events
                    SET status = 'skipped',
                        sent_at = NULL,
                        description = description || ?
                    WHERE event_id IN ({placeholders})
                    """,
                    (f"\nSkipped unsent at startup: {reason}", *event_ids),
                )
                await connection.commit()
                return len(event_ids)
            except BaseException:
                await connection.rollback()
                raise

    async def cleanup_old_records(self, older_than: datetime) -> int:
        connection = self._require_connection()
        async with self._lock:
            await connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = await connection.execute(
                    """
                    SELECT event_id
                    FROM smartshell_events
                    WHERE created_at < ?
                      AND status IN ('sent', 'skipped')
                    """,
                    (_format_dt(older_than),),
                )
                event_ids = [int(row[0]) for row in await cursor.fetchall()]
                if not event_ids:
                    await connection.rollback()
                    return 0

                placeholders = ",".join("?" for _ in event_ids)
                await connection.execute(
                    f"DELETE FROM outbox WHERE event_id IN ({placeholders})",
                    tuple(event_ids),
                )
                await connection.execute(
                    f"DELETE FROM smartshell_events WHERE event_id IN ({placeholders})",
                    tuple(event_ids),
                )
                await connection.commit()
                await connection.execute("VACUUM")
                return len(event_ids)
            except BaseException:
                await connection.rollback()
                raise

    async def initialize_cursor(self, value: datetime) -> bool:
        return await self.initialize_state_cursor("smartshell_event_cursor", value)

    async def initialize_state_cursor(self, key: str, value: datetime) -> bool:
        connection = self._require_connection()
        async with self._lock:
            cursor = await connection.execute(
                """
                INSERT OR IGNORE INTO state (key, value)
                VALUES (?, ?)
                """,
                (key, _format_dt(value)),
            )
            await connection.commit()
            return cursor.rowcount == 1

    async def get_cursor(self) -> datetime:
        return await self.get_state_cursor("smartshell_event_cursor")

    async def get_state_cursor(self, key: str) -> datetime:
        connection = self._require_connection()
        async with self._lock:
            cursor = await connection.execute(
                "SELECT value FROM state WHERE key = ?",
                (key,),
            )
            row = await cursor.fetchone()
        if row is None:
            raise RuntimeError(f"State cursor {key} is not initialized")
        return _parse_dt(str(row["value"]))

    async def advance_cursor(self, value: datetime) -> None:
        await self.advance_state_cursor("smartshell_event_cursor", value)

    async def advance_state_cursor(self, key: str, value: datetime) -> None:
        current = await self.get_state_cursor(key)
        if value <= current:
            return
        await self._update_state(key, _format_dt(value))

    async def enqueue_event(
        self,
        event_id: int,
        created_at: datetime,
        event_type: str,
        description: str,
        message_text: str,
    ) -> bool:
        parts = split_for_max(message_text)
        if not parts:
            return False

        connection = self._require_connection()
        async with self._lock:
            await connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = await connection.execute(
                    """
                    INSERT OR IGNORE INTO smartshell_events
                        (event_id, created_at, event_type, description, status, inserted_at)
                    VALUES (?, ?, ?, ?, 'queued', ?)
                    """,
                    (event_id, _format_dt(created_at), event_type, description, time.time()),
                )
                if cursor.rowcount == 0:
                    await connection.rollback()
                    return False

                await connection.executemany(
                    """
                    INSERT INTO outbox (event_id, part_index, message_text)
                    VALUES (?, ?, ?)
                    """,
                    [(event_id, index, part) for index, part in enumerate(parts)],
                )
                await connection.commit()
                return True
            except BaseException:
                await connection.rollback()
                raise

    async def mark_skipped(
        self,
        event_id: int,
        created_at: datetime,
        event_type: str,
        description: str,
    ) -> None:
        connection = self._require_connection()
        async with self._lock:
            await connection.execute(
                """
                INSERT OR IGNORE INTO smartshell_events
                    (event_id, created_at, event_type, description, status, inserted_at)
                VALUES (?, ?, ?, ?, 'skipped', ?)
                """,
                (event_id, _format_dt(created_at), event_type, description, time.time()),
            )
            await connection.commit()

    async def next_outbox_item(self) -> OutboxItem | None:
        connection = self._require_connection()
        async with self._lock:
            cursor = await connection.execute(
                """
                SELECT
                    o.id,
                    o.event_id,
                    o.part_index,
                    o.message_text,
                    o.status,
                    o.attempt_count,
                    o.next_attempt_at,
                    o.attempt_started_at
                FROM outbox AS o
                JOIN smartshell_events AS e ON e.event_id = o.event_id
                WHERE o.status IN ('pending', 'sending', 'uncertain')
                  AND o.next_attempt_at <= ?
                ORDER BY e.created_at, o.event_id, o.part_index
                LIMIT 1
                """,
                (time.time(),),
            )
            row = await cursor.fetchone()
        return OutboxItem(**dict(row)) if row is not None else None

    async def mark_sending(self, item_id: int) -> None:
        await self._execute(
            """
            UPDATE outbox
            SET status = 'sending',
                attempt_count = attempt_count + 1,
                attempt_started_at = ?,
                last_error = NULL
            WHERE id = ?
            """,
            (time.time(), item_id),
        )

    async def mark_retry(
        self,
        item_id: int,
        next_attempt_at: float,
        error: str,
        *,
        uncertain: bool,
    ) -> None:
        status = "uncertain" if uncertain else "pending"
        await self._execute(
            """
            UPDATE outbox
            SET status = ?,
                next_attempt_at = ?,
                last_error = ?
            WHERE id = ?
            """,
            (status, next_attempt_at, error[:2000], item_id),
        )

    async def mark_sent(self, item_id: int, max_message_id: str | None) -> None:
        connection = self._require_connection()
        async with self._lock:
            await connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = await connection.execute("SELECT event_id FROM outbox WHERE id = ?", (item_id,))
                row = await cursor.fetchone()
                if row is None:
                    await connection.rollback()
                    return
                event_id = int(row["event_id"])
                await connection.execute(
                    """
                    UPDATE outbox
                    SET status = 'sent',
                        next_attempt_at = 0,
                        max_message_id = ?,
                        attempt_started_at = NULL,
                        last_error = NULL
                    WHERE id = ?
                    """,
                    (max_message_id, item_id),
                )
                cursor = await connection.execute(
                    "SELECT COUNT(*) FROM outbox WHERE event_id = ? AND status != 'sent'",
                    (event_id,),
                )
                pending_count = int((await cursor.fetchone())[0])
                if pending_count == 0:
                    await connection.execute(
                        """
                        UPDATE smartshell_events
                        SET status = 'sent', sent_at = ?
                        WHERE event_id = ?
                        """,
                        (time.time(), event_id),
                    )
                await connection.commit()
            except BaseException:
                await connection.rollback()
                raise

    async def mark_failed(self, item_id: int, error: str) -> None:
        await self._execute(
            """
            UPDATE outbox
            SET status = 'failed',
                last_error = ?
            WHERE id = ?
            """,
            (error[:2000], item_id),
        )

    async def reset_uncertain_to_pending(self, item_id: int) -> None:
        await self._execute(
            """
            UPDATE outbox
            SET status = 'pending',
                next_attempt_at = 0,
                last_error = 'MAX reconciliation found no matching message'
            WHERE id = ?
            """,
            (item_id,),
        )

    async def claimed_max_message_ids(self) -> set[str]:
        connection = self._require_connection()
        async with self._lock:
            cursor = await connection.execute(
                """
                SELECT max_message_id
                FROM outbox
                WHERE max_message_id IS NOT NULL
                ORDER BY id DESC
                LIMIT 500
                """
            )
            rows = await cursor.fetchall()
        return {str(row[0]) for row in rows}

    async def _update_state(self, key: str, value: str) -> None:
        await self._execute(
            """
            INSERT INTO state (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )

    async def _execute(self, sql: str, parameters: tuple[object, ...]) -> None:
        connection = self._require_connection()
        async with self._lock:
            await connection.execute(sql, parameters)
            await connection.commit()

    def _require_connection(self) -> aiosqlite.Connection:
        if self._connection is None:
            raise RuntimeError("Storage is not open")
        return self._connection


def _format_dt(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _parse_dt(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
