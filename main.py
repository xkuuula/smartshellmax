from __future__ import annotations

import asyncio
from collections import Counter
from html.parser import HTMLParser
import logging
import random
import signal
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from config import Config
from logger import configure_logging
from max_client import MaxApiError, MaxClient
from smartshell_client import (
    GoodHistoryOperation,
    SmartShellClient,
    SmartShellEvent,
    SmartShellError,
    WorkShiftOverview,
)
from storage import OutboxItem, Storage


logger = logging.getLogger(__name__)

ALWAYS_FORWARD_EVENT_TYPES = {
    "SHELL_HIGH_ACCESS_ENABLED",
    "SHELL_HIGH_ACCESS_DISABLED",
    "SHELL_DISABLED",
    "SHELL_ENABLED",
}


async def main_async() -> None:
    config = Config.load()
    configure_logging(config.log_file, config.log_level)
    logger.info("Starting SmartShell to MAX service")
    logger.info(
        "Configured SmartShell company_id=%s, timezone=%s, target MAX chat_id=%s",
        config.smartshell_company_id,
        config.smartshell_timezone,
        config.max_target_chat_id,
    )

    storage = Storage(config.database_path)
    await storage.open()
    await storage.recover_after_restart()
    first_run = await storage.initialize_cursor(_club_now(config))
    warehouse_first_run = await storage.initialize_state_cursor("smartshell_warehouse_cursor", _club_now(config))

    stop_event = asyncio.Event()
    _install_signal_handlers(stop_event)

    try:
        async with SmartShellClient(config) as smartshell, MaxClient(config) as max_client:
            sender = asyncio.create_task(outbox_worker(storage, max_client, stop_event), name="max-sender")
            poller = asyncio.create_task(
                poll_smartshell(config, storage, smartshell, stop_event, first_run),
                name="smartshell-poller",
            )
            warehouse_poller = asyncio.create_task(
                poll_smartshell_warehouse(config, storage, smartshell, stop_event, warehouse_first_run),
                name="smartshell-warehouse-poller",
            )
            try:
                await stop_event.wait()
            finally:
                poller.cancel()
                warehouse_poller.cancel()
                sender.cancel()
                await asyncio.gather(poller, warehouse_poller, sender, return_exceptions=True)
    finally:
        await storage.close()
        logger.info("Service stopped")


async def poll_smartshell(
    config: Config,
    storage: Storage,
    smartshell: SmartShellClient,
    stop_event: asyncio.Event,
    first_run: bool,
) -> None:
    failures = 0

    while not stop_event.is_set():
        try:
            cursor = await storage.get_cursor()
            start = cursor if first_run else cursor - timedelta(minutes=config.smartshell_poll_window_minutes)
            finish = _club_now(config) + timedelta(minutes=1)
            events = await smartshell.fetch_events(start, finish)
            events = await _with_forced_shell_events(smartshell, start, finish, events)
            logger.info(
                "Fetched %s SmartShell event(s) from %s to %s; types=%s",
                len(events),
                _format_log_dt(start),
                _format_log_dt(finish),
                _event_type_summary(events),
            )
            queued = await enqueue_new_events(config, storage, smartshell, events)
            if queued:
                logger.info("Queued %s SmartShell event(s) for MAX", queued)
            failures = 0
            first_run = False
            await _sleep_or_stop(stop_event, config.smartshell_poll_interval_seconds)
        except SmartShellError as error:
            failures += 1
            logger.error("SmartShell polling failed: %s", error)
            await _sleep_or_stop(stop_event, _backoff(failures, ceiling=300))
        except Exception:
            failures += 1
            logger.exception("Unexpected SmartShell polling error")
            await _sleep_or_stop(stop_event, _backoff(failures, ceiling=300))


async def _with_forced_shell_events(
    smartshell: SmartShellClient,
    start: datetime,
    finish: datetime,
    events: list[SmartShellEvent],
) -> list[SmartShellEvent]:
    try:
        shell_events = await smartshell.fetch_events(start, finish, ALWAYS_FORWARD_EVENT_TYPES)
    except SmartShellError as error:
        logger.warning("Forced SmartShell shell-event polling failed: %s", error)
        return events

    merged: dict[int, SmartShellEvent] = {event.id: event for event in events}
    for event in shell_events:
        merged[event.id] = event
    if shell_events:
        logger.info(
            "Forced shell-event check returned %s event(s); types=%s",
            len(shell_events),
            _event_type_summary(shell_events),
        )
    return list(merged.values())


async def poll_smartshell_warehouse(
    config: Config,
    storage: Storage,
    smartshell: SmartShellClient,
    stop_event: asyncio.Event,
    first_run: bool,
) -> None:
    failures = 0

    while not stop_event.is_set():
        try:
            cursor = await storage.get_state_cursor("smartshell_warehouse_cursor")
            start = cursor if first_run else cursor - timedelta(minutes=config.smartshell_poll_window_minutes)
            finish = _club_now(config) + timedelta(minutes=1)
            logger.info(
                "Checking SmartShell warehouse operations from %s to %s",
                _format_log_dt(start),
                _format_log_dt(finish),
            )
            queued = await enqueue_good_history_operations(
                config,
                storage,
                smartshell,
                start,
                finish,
                stop_event,
            )
            await storage.advance_state_cursor("smartshell_warehouse_cursor", finish)
            if queued:
                logger.info("Queued %s SmartShell warehouse operation(s) for MAX", queued)
            failures = 0
            first_run = False
            await _sleep_or_stop(stop_event, config.smartshell_warehouse_poll_interval_seconds)
        except SmartShellError as error:
            failures += 1
            logger.error("SmartShell warehouse polling failed: %s", error)
            await _sleep_or_stop(stop_event, _backoff(failures, ceiling=300))
        except Exception:
            failures += 1
            logger.exception("Unexpected SmartShell warehouse polling error")
            await _sleep_or_stop(stop_event, _backoff(failures, ceiling=300))


async def enqueue_new_events(
    config: Config,
    storage: Storage,
    smartshell: SmartShellClient,
    events: list[SmartShellEvent],
) -> int:
    queued = 0
    for event in sorted(events, key=lambda item: (item.created_at, item.id)):
        if not _should_forward(config, event):
            logger.info(
                "Skipped SmartShell event id=%s type=%s at=%s by filter: %s",
                event.id,
                event.type,
                _format_log_dt(event.created_at),
                _compact_log(_plain_text(event.description)),
            )
            await storage.mark_skipped(event.id, event.created_at, event.type, event.description)
            await storage.advance_cursor(event.created_at)
            continue

        text = await _format_event_message(config, smartshell, event)
        inserted = await storage.enqueue_event(event.id, event.created_at, event.type, event.description, text)
        await storage.advance_cursor(event.created_at)
        if inserted:
            queued += 1
            logger.info(
                "Queued SmartShell event id=%s type=%s at=%s",
                event.id,
                event.type,
                _format_log_dt(event.created_at),
            )
        else:
            logger.info(
                "Ignored duplicate SmartShell event id=%s type=%s at=%s",
                event.id,
                event.type,
                _format_log_dt(event.created_at),
            )
    return queued


async def enqueue_good_history_operations(
    config: Config,
    storage: Storage,
    smartshell: SmartShellClient,
    start: datetime,
    finish: datetime,
    stop_event: asyncio.Event,
) -> int:
    queued = 0
    try:
        goods = await smartshell.fetch_goods()
    except SmartShellError as error:
        logger.warning("SmartShell goods polling skipped: %s", error)
        return 0
    logger.info("Fetched %s SmartShell goods for warehouse history check", len(goods))

    for index, good in enumerate(goods):
        try:
            operations = await smartshell.fetch_good_history(good, start, finish)
        except SmartShellError as error:
            logger.warning("SmartShell goodHistory polling skipped for good_id=%s: %s", good.id, error)
            continue
        for operation in sorted(operations, key=lambda item: (item.created_at, item.id)):
            text = _format_good_history_message(config, operation)
            inserted = await storage.enqueue_event(
                operation.id,
                operation.created_at,
                f"GOOD_HISTORY_{operation.operation}",
                _good_history_description(operation),
                text,
            )
            if inserted:
                queued += 1
                logger.info(
                    "Queued SmartShell good history operation id=%s good_id=%s operation=%s at=%s",
                    operation.id,
                    operation.good_id,
                    operation.operation,
                    _format_log_dt(operation.created_at),
                )
            else:
                logger.info(
                    "Ignored duplicate SmartShell good history operation id=%s good_id=%s operation=%s at=%s",
                    operation.id,
                    operation.good_id,
                    operation.operation,
                    _format_log_dt(operation.created_at),
                )
        if index < len(goods) - 1:
            await _sleep_or_stop(stop_event, 4)
    return queued


async def outbox_worker(storage: Storage, max_client: MaxClient, stop_event: asyncio.Event) -> None:
    failures = 0
    while not stop_event.is_set():
        item = await storage.next_outbox_item()
        if item is None:
            failures = 0
            await _sleep_or_stop(stop_event, 3)
            continue

        delay = item.next_attempt_at - time.time()
        if delay > 0:
            await _sleep_or_stop(stop_event, min(delay, 30))
            continue

        if item.status == "uncertain":
            reconciled = await reconcile_uncertain_send(storage, max_client, item)
            if reconciled:
                continue

        await storage.mark_sending(item.id)
        try:
            sent = await max_client.send_text(item.message_text)
        except MaxApiError as error:
            if not error.transient:
                logger.error("Permanent MAX error for SmartShell event id=%s: %s", item.event_id, error)
                await storage.mark_failed(item.id, str(error))
                continue
            failures += 1
            wait = error.retry_after if error.retry_after is not None else _backoff(item.attempt_count + failures)
            logger.error("MAX send failed for SmartShell event id=%s: %s", item.event_id, error)
            await storage.mark_retry(item.id, time.time() + wait, str(error), uncertain=error.uncertain)
            continue
        except Exception:
            failures += 1
            logger.exception("Unexpected MAX sender error for SmartShell event id=%s", item.event_id)
            await storage.mark_retry(
                item.id,
                time.time() + _backoff(item.attempt_count + failures),
                "Unexpected MAX sender error; see log",
                uncertain=True,
            )
            continue

        failures = 0
        await storage.mark_sent(item.id, sent.message_id)
        logger.info(
            "Sent SmartShell event id=%s to MAX (MAX message id=%s)",
            item.event_id,
            sent.message_id or "not returned",
        )
        await _sleep_or_stop(stop_event, 0.6)


async def reconcile_uncertain_send(
    storage: Storage,
    max_client: MaxClient,
    item: OutboxItem,
) -> bool:
    attempt_started_at = item.attempt_started_at or (time.time() - 3600)
    try:
        claimed_ids = await storage.claimed_max_message_ids()
        matched_id = await max_client.find_matching_message(
            item.message_text,
            attempt_started_at,
            claimed_ids,
        )
    except MaxApiError as error:
        logger.error("MAX reconciliation failed for SmartShell event id=%s: %s", item.event_id, error)
        wait = error.retry_after if error.retry_after is not None else 30
        await storage.mark_retry(item.id, time.time() + wait, str(error), uncertain=True)
        return True

    if matched_id is not None:
        await storage.mark_sent(item.id, matched_id)
        logger.info(
            "Recovered successful MAX send for SmartShell event id=%s (MAX message id=%s)",
            item.event_id,
            matched_id,
        )
        return True

    await storage.reset_uncertain_to_pending(item.id)
    logger.warning(
        "No matching MAX message found for uncertain SmartShell event id=%s; sending again",
        item.event_id,
    )
    return False


def _should_forward(config: Config, event: SmartShellEvent) -> bool:
    if event.type == "WORK_SHIFT_FINISHED" and event.work_shift_id is not None:
        return True
    if event.type.upper() in ALWAYS_FORWARD_EVENT_TYPES:
        return True
    if config.smartshell_send_all_events:
        return True
    if config.smartshell_event_types and event.type.upper() in config.smartshell_event_types:
        return True
    if config.smartshell_description_keywords:
        haystack = _plain_text(event.description).casefold()
        return any(keyword.casefold() in haystack for keyword in config.smartshell_description_keywords)
    return True


async def _format_event_message(
    config: Config,
    smartshell: SmartShellClient,
    event: SmartShellEvent,
) -> str:
    if event.type == "WORK_SHIFT_FINISHED" and event.work_shift_id is not None:
        overview = await smartshell.fetch_work_shift_overview(event.work_shift_id)
        return _format_work_shift_report(config, overview)

    club_title = config.smartshell_club_title or str(config.smartshell_company_id)
    timestamp = event.created_at.strftime("%H:%M %d.%m.%Y")
    return f"Клуб: {config.smartshell_company_id} | {club_title}\n\n{timestamp}\n{event.description}"


def _format_work_shift_report(config: Config, overview: WorkShiftOverview) -> str:
    club_title = config.smartshell_club_title or str(config.smartshell_company_id)
    currency = _currency_symbol(overview.currency_alias)
    cash_in_register = (
        overview.cash_on_start
        + overview.cash
        + overview.cash_order_income
        - overview.cash_order_expenses
    )
    operator_name = " ".join(
        part
        for part in (
            overview.worker_last_name,
            overview.worker_first_name,
        )
        if part
    )
    operator = operator_name or overview.worker_nickname
    if overview.worker_phone:
        operator = f"{operator} ({overview.worker_phone})" if operator else overview.worker_phone
    session_cash = overview.tariff_sum - overview.deposit + overview.tips

    lines = [
        f"Клуб: {config.smartshell_company_id} | {club_title}",
        "",
        _format_report_dt(overview.finished_at),
        "-----------------------------",
        f"Начало: {_format_report_dt(overview.created_at)}",
        f"Завершение: {_format_report_dt(overview.finished_at)}",
        "-----------------------------",
        "Финансы:",
        f"Выручка: {_money(overview.total)}{currency}",
        f"Наличными: {_money(overview.cash)}{currency}",
        f"Картой: {_money(overview.card)}{currency}",
        f"Онлайн: {_money(overview.online_deposit)}{currency}",
        f"Наличных в кассе: {_money(cash_in_register)}{currency}",
        f"На начало смены: {_money(overview.cash_on_start)}{currency}",
        "-----------------------------",
        "Статистика:",
        "",
        "Сеансы:",
        f"-Касса: {_money(session_cash)}{currency}",
        f"-Траты с депозита: {_money(overview.deposit)}{currency}",
        "",
        "Товары:",
        f"-Сумма: {_money(overview.goods_sum)}{currency}",
        "",
        "Услуги:",
        f"-Сумма: {_money(overview.services_sum)}{currency}",
        "",
        f"Пополнения депозита: {_money(overview.deposit_sum)}{currency}",
        f"Бонусные пополнения: {_money(overview.bonus)} ⊙",
        f"Отмен на сумму: {_money(overview.refunded)}{currency}",
    ]
    if overview.cash_order_expenses or overview.cash_order_income:
        lines.extend(
            [
                "-----------------------------",
                "Дополнительно:",
            ]
        )
        if overview.cash_order_expenses:
            lines.append(f"Расходных ордеров: {_money_compact(overview.cash_order_expenses)}{currency}")
        if overview.cash_order_income:
            lines.append(f"Приходных ордеров: {_money_compact(overview.cash_order_income)}{currency}")
    lines.extend(
        [
            "-----------------------------",
            f"Смена № {overview.id}",
            f"Оператор: {operator}".rstrip(),
        ]
    )
    return "\n".join(lines)


def _format_good_history_message(config: Config, operation: GoodHistoryOperation) -> str:
    club_title = config.smartshell_club_title or str(config.smartshell_company_id)
    timestamp = operation.created_at.strftime("%H:%M %d.%m.%Y")
    employee = _format_person(
        operation.initiator_last_name,
        operation.initiator_first_name,
        operation.initiator_nickname,
    )
    before = operation.quantity_after - operation.delta
    quantity = abs(operation.delta)
    if operation.operation == "ADD":
        action = "оприходовал товары на склад"
        line = f"{operation.good_title}: + {quantity} шт. (Стало {operation.quantity_after} шт.)"
    elif operation.operation == "DISPOSAL":
        action = "списал товары со склада"
        line = f"{operation.good_title}: - {quantity} шт. (Стало {operation.quantity_after} шт.)"
    else:
        action = "изменил товары на складе"
        sign = "+" if operation.delta >= 0 else "-"
        line = f"{operation.good_title}: {sign} {quantity} шт. (Стало {operation.quantity_after} шт.)"

    lines = [
        f"Клуб: {config.smartshell_company_id} | {club_title}",
        "",
        timestamp,
        f"Сотрудник {employee} {action}:",
        "",
        line,
    ]
    if operation.comment:
        lines.append(f"Комментарий: {operation.comment}")
    return "\n".join(lines)


def _good_history_description(operation: GoodHistoryOperation) -> str:
    return (
        f"{operation.operation} {operation.good_title} "
        f"delta={operation.delta} before={operation.quantity_after - operation.delta} "
        f"after={operation.quantity_after} comment={operation.comment}"
    )


def _format_person(last_name: str, first_name: str, fallback: str) -> str:
    name = " ".join(part for part in (last_name, first_name) if part)
    return name or fallback or "Неизвестный сотрудник"


def _format_report_dt(value: datetime) -> str:
    return value.strftime("%H:%M %d.%m.%Y")


def _format_log_dt(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _club_now(config: Config) -> datetime:
    try:
        timezone = ZoneInfo(config.smartshell_timezone)
    except ZoneInfoNotFoundError as error:
        raise RuntimeError(f"Unknown SMARTSHELL_TIMEZONE: {config.smartshell_timezone}") from error
    return datetime.now(timezone).replace(tzinfo=None)


def _event_type_summary(events: list[SmartShellEvent]) -> str:
    if not events:
        return "none"
    counts = Counter(event.type for event in events)
    return ", ".join(f"{event_type}:{count}" for event_type, count in counts.most_common(12))


def _money(value: float) -> str:
    return f"{value:.2f}"


def _money_compact(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else _money(value)


def _currency_symbol(alias: str) -> str:
    return "₽" if alias.upper() in {"RUB", "RUR"} else alias


def _plain_text(value: str) -> str:
    parser = _PlainTextParser()
    parser.feed(value)
    parser.close()
    return parser.result()


def _compact_log(value: str, limit: int = 180) -> str:
    compacted = " ".join(value.split())
    if len(compacted) <= limit:
        return compacted
    return compacted[: limit - 3] + "..."


class _PlainTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "br":
            self._chunks.append("\n")

    def handle_data(self, data: str) -> None:
        self._chunks.append(data)

    def result(self) -> str:
        return "".join(self._chunks)


def _backoff(attempt: int, *, ceiling: float = 120.0) -> float:
    base = min(ceiling, 2.0 ** min(max(attempt, 1), 8))
    return random.uniform(max(1.0, base / 2), base)


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=max(0.05, seconds))
    except asyncio.TimeoutError:
        pass


def _install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop_event.set())


def main() -> None:
    try:
        asyncio.run(main_async())
    except (KeyboardInterrupt, SystemExit):
        pass
    except ValueError as error:
        print(f"Configuration error: {error}", file=sys.stderr)
        raise SystemExit(2) from error


if __name__ == "__main__":
    main()
