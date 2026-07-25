from __future__ import annotations

import json
import logging
import hashlib
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import aiohttp

from config import Config


logger = logging.getLogger(__name__)


class SmartShellError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class SmartShellEvent:
    id: int
    type: str
    created_at: datetime
    description: str
    work_shift_id: int | None = None


@dataclass(frozen=True, slots=True)
class Good:
    id: int
    title: str
    amount: int


@dataclass(frozen=True, slots=True)
class GoodHistoryOperation:
    id: int
    good_id: int
    good_title: str
    operation: str
    created_at: datetime
    delta: int
    quantity_after: int
    comment: str
    initiator_first_name: str
    initiator_last_name: str
    initiator_nickname: str


@dataclass(frozen=True, slots=True)
class WorkShiftOverview:
    id: int
    worker_nickname: str
    worker_uuid: str
    worker_first_name: str
    worker_last_name: str
    worker_middle_name: str
    worker_phone: str
    cash_on_start: float
    total: float
    deposit: float
    online_deposit: float
    bonus: float
    refunded: float
    cash: float
    card: float
    goods_sum: float
    services_sum: float
    tariff_sum: float
    deposit_sum: float
    tips: float
    cash_order_expenses: float
    cash_order_income: float
    currency_alias: str
    created_at: datetime
    finished_at: datetime


class SmartShellClient:
    def __init__(self, config: Config) -> None:
        self._api_url = config.smartshell_api_url
        self._login = config.smartshell_login
        self._password = config.smartshell_password
        self._company_id = config.smartshell_company_id
        self._page_size = config.smartshell_page_size
        self._max_pages_per_poll = config.smartshell_max_pages_per_poll
        self._event_types = sorted(config.smartshell_event_types)
        self._session: aiohttp.ClientSession | None = None
        self._access_token: str | None = None
        self._token_type = "Bearer"
        self._token_expires_at = 0.0

    async def __aenter__(self) -> "SmartShellClient":
        timeout = aiohttp.ClientTimeout(total=40, connect=10, sock_read=30)
        self._session = aiohttp.ClientSession(timeout=timeout, headers={"Accept": "application/json"})
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def login(self) -> None:
        query = (
            "mutation login {"
            " login(input:{"
            f" login:{json.dumps(self._login)}"
            f" password:{json.dumps(self._password)}"
            f" company_id:{self._company_id}"
            " }) { access_token token_type refresh_token expires_in }"
            "}"
        )
        payload = await self._graphql(query, authorized=False)
        data = payload.get("data", {}).get("login")
        if not isinstance(data, dict) or not data.get("access_token"):
            raise SmartShellError("SmartShell login response does not contain access_token")

        self._access_token = str(data["access_token"])
        self._token_type = str(data.get("token_type") or "Bearer")
        expires_in = int(data.get("expires_in") or 3600)
        self._token_expires_at = time.time() + max(60, expires_in - 120)
        logger.info("SmartShell authenticated, token expires in %s seconds", expires_in)

    async def fetch_events(
        self,
        start: datetime,
        finish: datetime,
        event_types: set[str] | None = None,
    ) -> list[SmartShellEvent]:
        await self._ensure_token()
        input_parts = [
            f"start:{json.dumps(_format_dt(start))}",
            f"finish:{json.dumps(_format_dt(finish))}",
        ]
        effective_event_types = sorted(event_types) if event_types is not None else self._event_types
        if effective_event_types:
            event_type_values = ", ".join(json.dumps(event_type) for event_type in effective_event_types)
            input_parts.append(f"types:[{event_type_values}]")

        events: list[SmartShellEvent] = []
        for page in range(1, self._max_pages_per_poll + 1):
            query = (
                "query eventList {"
                f" eventList(input:{{ {' '.join(input_parts)} }}, page:{page}, first:{self._page_size})"
                " { data { timestamp type description work_shift { id } } }"
                "}"
            )

            try:
                payload = await self._graphql(query, authorized=True)
            except SmartShellError as error:
                if _looks_like_auth_error(str(error)):
                    logger.warning("SmartShell token was rejected; logging in again")
                    await self.login()
                    payload = await self._graphql(query, authorized=True)
                else:
                    raise

            raw_events = payload.get("data", {}).get("eventList", {}).get("data", [])
            if not isinstance(raw_events, list):
                raise SmartShellError("SmartShell eventList response does not contain data list")

            for item in raw_events:
                if not isinstance(item, dict):
                    continue
                try:
                    timestamp = str(item["timestamp"])
                    event_type = str(item.get("type") or "")
                    description = str(item.get("description") or "").strip()
                    work_shift = item.get("work_shift")
                    work_shift_id = _optional_int(work_shift.get("id")) if isinstance(work_shift, dict) else None
                    event = SmartShellEvent(
                        id=_stable_event_id(timestamp, event_type, description),
                        type=event_type,
                        created_at=_parse_dt(timestamp),
                        description=description,
                        work_shift_id=work_shift_id,
                    )
                except (KeyError, TypeError, ValueError) as error:
                    logger.warning("Skipping malformed SmartShell event: %s", error)
                    continue
                if event.description:
                    events.append(event)

            if len(raw_events) < self._page_size:
                break

        if len(events) >= self._page_size * self._max_pages_per_poll:
            logger.warning(
                "SmartShell returned at least %s events in one poll; increase poll frequency or max pages",
                len(events),
            )
        logger.debug("Fetched %s SmartShell event(s)", len(events))
        return events

    async def fetch_goods(self) -> list[Good]:
        await self._ensure_token()
        query = (
            "query goods {"
            " goods(input:{})"
            " { id title amount }"
            "}"
        )
        payload = await self._graphql(query, authorized=True)
        raw_goods = payload.get("data", {}).get("goods", [])
        if not isinstance(raw_goods, list):
            raise SmartShellError("SmartShell goods response does not contain list")

        goods: list[Good] = []
        for item in raw_goods:
            if not isinstance(item, dict):
                continue
            try:
                goods.append(
                    Good(
                        id=int(item["id"]),
                        title=str(item.get("title") or ""),
                        amount=int(item.get("amount") or 0),
                    )
                )
            except (KeyError, TypeError, ValueError) as error:
                logger.warning("Skipping malformed SmartShell good: %s", error)
        return goods

    async def fetch_good_history(
        self,
        good: Good,
        start: datetime,
        finish: datetime,
    ) -> list[GoodHistoryOperation]:
        await self._ensure_token()
        query = (
            "query goodHistory {"
            " goodHistory(input:{"
            f" id:{good.id}"
            f" from:{json.dumps(_format_dt(start))}"
            f" to:{json.dumps(_format_dt(finish))}"
            " operations:[ADD,DISPOSAL]"
            " page:1"
            " first:100"
            " })"
            " { data {"
            " created_at operation delta quantity_after comment"
            " initiator { nickname first_name last_name }"
            " } }"
            "}"
        )
        payload = await self._graphql(query, authorized=True)
        raw_operations = payload.get("data", {}).get("goodHistory", {}).get("data", [])
        if not isinstance(raw_operations, list):
            raise SmartShellError("SmartShell goodHistory response does not contain data list")

        operations: list[GoodHistoryOperation] = []
        for item in raw_operations:
            if not isinstance(item, dict):
                continue
            try:
                created_at = _parse_dt(str(item["created_at"]))
                operation = str(item.get("operation") or "")
                delta = int(item.get("delta") or 0)
                quantity_after = int(item.get("quantity_after") or 0)
                comment = str(item.get("comment") or "").strip()
                initiator = item.get("initiator") if isinstance(item.get("initiator"), dict) else {}
                operations.append(
                    GoodHistoryOperation(
                        id=_stable_good_history_id(
                            good.id,
                            operation,
                            delta,
                            quantity_after,
                            comment,
                        ),
                        good_id=good.id,
                        good_title=good.title,
                        operation=operation,
                        created_at=created_at,
                        delta=delta,
                        quantity_after=quantity_after,
                        comment=comment,
                        initiator_first_name=str(initiator.get("first_name") or ""),
                        initiator_last_name=str(initiator.get("last_name") or ""),
                        initiator_nickname=str(initiator.get("nickname") or ""),
                    )
                )
            except (KeyError, TypeError, ValueError) as error:
                logger.warning("Skipping malformed SmartShell good history operation: %s", error)
        return operations

    async def fetch_work_shift_overview(self, work_shift_id: int) -> WorkShiftOverview:
        await self._ensure_token()
        query = (
            "query workShiftOverview {"
            f" getWorkShiftPaymentOverviewData(id:{work_shift_id})"
            " {"
            " id worker { uuid nickname first_name last_name middle_name phone }"
            " cash_on_start total deposit online_deposit"
            " bonus refunded cash card created_at finished_at"
            " sum { good service tariff deposit } currency { alias }"
            " }"
            f" getDetailedWorkShiftMoneyData(id:{work_shift_id})"
            " {"
            " cash_orders { type sum }"
            " }"
            "}"
        )
        payload = await self._graphql(query, authorized=True)
        data = payload.get("data", {}).get("getWorkShiftPaymentOverviewData")
        if not isinstance(data, dict):
            raise SmartShellError(f"SmartShell returned no shift overview for id={work_shift_id}")
        detailed = payload.get("data", {}).get("getDetailedWorkShiftMoneyData")
        cash_orders = detailed.get("cash_orders") if isinstance(detailed, dict) else []
        cash_order_expenses = _sum_cash_orders(cash_orders, "RKO")
        cash_order_income = _sum_cash_orders(cash_orders, "PKO")

        worker = data.get("worker") if isinstance(data.get("worker"), dict) else {}
        sums = data.get("sum") if isinstance(data.get("sum"), dict) else {}
        currency = data.get("currency") if isinstance(data.get("currency"), dict) else {}
        created_at = _parse_dt(str(data["created_at"]))
        finished_at = _parse_dt(str(data["finished_at"]))
        worker_uuid = str(worker.get("uuid") or "")
        tips = await self._fetch_work_shift_tips(created_at, finished_at, worker_uuid)

        return WorkShiftOverview(
            id=int(data["id"]),
            worker_nickname=str(worker.get("nickname") or ""),
            worker_uuid=worker_uuid,
            worker_first_name=str(worker.get("first_name") or ""),
            worker_last_name=str(worker.get("last_name") or ""),
            worker_middle_name=str(worker.get("middle_name") or ""),
            worker_phone=str(worker.get("phone") or ""),
            cash_on_start=_float(data.get("cash_on_start")),
            total=_float(data.get("total")),
            deposit=_float(data.get("deposit")),
            online_deposit=_float(data.get("online_deposit")),
            bonus=_float(data.get("bonus")),
            refunded=_float(data.get("refunded")),
            cash=_float(data.get("cash")),
            card=_float(data.get("card")),
            goods_sum=_float(sums.get("good")),
            services_sum=_float(sums.get("service")),
            tariff_sum=_float(sums.get("tariff")),
            deposit_sum=_float(sums.get("deposit")),
            tips=tips,
            cash_order_expenses=cash_order_expenses,
            cash_order_income=cash_order_income,
            currency_alias=str(currency.get("alias") or "RUB"),
            created_at=created_at,
            finished_at=finished_at,
        )

    async def _fetch_work_shift_tips(
        self,
        created_at: datetime,
        finished_at: datetime,
        worker_uuid: str,
    ) -> float:
        if not worker_uuid:
            return 0.0
        query = (
            "query workShiftSummary {"
            " workShiftsSummaryReport(input:{"
            f" from:{json.dumps(_format_dt(created_at))}"
            f" to:{json.dumps(_format_dt(finished_at))}"
            f" workerUuid:{json.dumps(worker_uuid)}"
            " }) { labels data { values } }"
            "}"
        )
        payload = await self._graphql(query, authorized=True)
        report = payload.get("data", {}).get("workShiftsSummaryReport")
        if not isinstance(report, dict):
            return 0.0
        labels = report.get("labels")
        rows = report.get("data")
        if not isinstance(labels, list) or not rows:
            return 0.0
        first_row = rows[0] if isinstance(rows, list) else None
        values = first_row.get("values") if isinstance(first_row, dict) else None
        if not isinstance(values, list):
            return 0.0

        value_labels = [str(label) for label in labels[1:]]
        for label, value in zip(value_labels, values):
            if label.casefold() == "чаевые":
                return _float(value)
        return 0.0

    async def _ensure_token(self) -> None:
        if self._access_token is None or time.time() >= self._token_expires_at:
            await self.login()

    async def _graphql(self, query: str, *, authorized: bool) -> dict[str, Any]:
        session = self._require_session()
        headers: dict[str, str] = {}
        if authorized:
            if not self._access_token:
                raise SmartShellError("SmartShell access token is missing")
            headers["Authorization"] = f"{self._token_type} {self._access_token}"

        try:
            async with session.post(self._api_url, json={"query": query}, headers=headers) as response:
                text = await response.text()
                if response.status >= 500 or response.status in {408, 409, 425, 429}:
                    raise SmartShellError(
                        f"SmartShell API temporary HTTP {response.status}: {_compact(text)}"
                    )
                if response.status >= 400:
                    raise SmartShellError(f"SmartShell API HTTP {response.status}: {_compact(text)}")
                try:
                    payload = await response.json(content_type=None)
                except (ValueError, aiohttp.ContentTypeError) as error:
                    raise SmartShellError("SmartShell API returned invalid JSON") from error
        except (aiohttp.ClientError, TimeoutError) as error:
            raise SmartShellError(f"SmartShell network error: {error}") from error

        errors = payload.get("errors")
        if errors:
            raise SmartShellError(f"SmartShell GraphQL error: {_compact(json.dumps(errors, ensure_ascii=False))}")
        return payload

    def _require_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("SmartShell client is not open")
        return self._session


def _format_dt(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _parse_dt(value: str) -> datetime:
    value = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(value[:26], fmt)
        except ValueError:
            continue
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)


def _compact(value: str) -> str:
    return " ".join(value.split())[:800] if value else "empty response"


def _looks_like_auth_error(value: str) -> bool:
    lowered = value.casefold()
    return any(marker in lowered for marker in ("unauth", "token", "authorization", "forbidden", "401"))


def _optional_int(value: object) -> int | None:
    try:
        if value is not None:
            return int(value)
    except (TypeError, ValueError):
        return None
    return None


def _float(value: object) -> float:
    try:
        if value is not None:
            return float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0


def _sum_cash_orders(cash_orders: object, order_type: str) -> float:
    if not isinstance(cash_orders, list):
        return 0.0
    total = 0.0
    for item in cash_orders:
        if not isinstance(item, dict):
            continue
        if str(item.get("type") or "").upper() == order_type:
            total += _float(item.get("sum"))
    return total


def _stable_event_id(timestamp: str, event_type: str, description: str) -> int:
    payload = f"{timestamp}\0{event_type}\0{description}".encode("utf-8", errors="replace")
    digest = hashlib.sha256(payload).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF


def _stable_good_history_id(
    good_id: int,
    operation: str,
    delta: int,
    quantity_after: int,
    comment: str,
) -> int:
    payload = (
        f"good_history\0{good_id}\0{operation}\0"
        f"{delta}\0{quantity_after}\0{comment}"
    ).encode("utf-8", errors="replace")
    digest = hashlib.sha256(payload).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF
