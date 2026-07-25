from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True, slots=True)
class Config:
    smartshell_api_url: str
    smartshell_login: str
    smartshell_password: str
    smartshell_company_id: int
    smartshell_club_title: str
    smartshell_timezone: str
    smartshell_poll_interval_seconds: float
    smartshell_poll_window_minutes: int
    smartshell_warehouse_poll_interval_seconds: float
    smartshell_page_size: int
    smartshell_max_pages_per_poll: int
    smartshell_event_types: set[str]
    smartshell_description_keywords: list[str]
    smartshell_send_all_events: bool
    max_api_base: str
    max_bot_token: str
    max_target_chat_id: int
    database_path: Path
    log_file: Path
    log_level: str

    @classmethod
    def load(cls) -> "Config":
        load_dotenv()

        poll_interval = _float_env("SMARTSHELL_POLL_INTERVAL_SECONDS", 15.0)
        if poll_interval < 3:
            raise ValueError("SMARTSHELL_POLL_INTERVAL_SECONDS must be at least 3")

        page_size = _int_env("SMARTSHELL_PAGE_SIZE", 50)
        if not 1 <= page_size <= 100:
            raise ValueError("SMARTSHELL_PAGE_SIZE must be between 1 and 100")
        max_pages_per_poll = _int_env("SMARTSHELL_MAX_PAGES_PER_POLL", 3)
        if not 1 <= max_pages_per_poll <= 10:
            raise ValueError("SMARTSHELL_MAX_PAGES_PER_POLL must be between 1 and 10")

        max_api_base = os.getenv("MAX_API_BASE", "https://platform-api2.max.ru").strip().rstrip("/")
        if not max_api_base.startswith("https://"):
            raise ValueError("MAX_API_BASE must use HTTPS")

        log_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if log_level not in valid_levels:
            raise ValueError(f"LOG_LEVEL must be one of: {', '.join(sorted(valid_levels))}")

        return cls(
            smartshell_api_url=_required_url("SMARTSHELL_API_URL"),
            smartshell_login=_required("SMARTSHELL_LOGIN"),
            smartshell_password=_required("SMARTSHELL_PASSWORD"),
            smartshell_company_id=_required_int("SMARTSHELL_COMPANY_ID"),
            smartshell_club_title=os.getenv("SMARTSHELL_CLUB_TITLE", "").strip(),
            smartshell_timezone=os.getenv("SMARTSHELL_TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow",
            smartshell_poll_interval_seconds=poll_interval,
            smartshell_poll_window_minutes=_int_env("SMARTSHELL_POLL_WINDOW_MINUTES", 120),
            smartshell_warehouse_poll_interval_seconds=_float_env(
                "SMARTSHELL_WAREHOUSE_POLL_INTERVAL_SECONDS",
                300.0,
            ),
            smartshell_page_size=page_size,
            smartshell_max_pages_per_poll=max_pages_per_poll,
            smartshell_event_types=_csv_set("SMARTSHELL_EVENT_TYPES"),
            smartshell_description_keywords=_csv_list("SMARTSHELL_DESCRIPTION_KEYWORDS"),
            smartshell_send_all_events=_bool_env("SMARTSHELL_SEND_ALL_EVENTS", False),
            max_api_base=max_api_base,
            max_bot_token=_required("MAX_BOT_TOKEN"),
            max_target_chat_id=_required_int("MAX_TARGET_CHAT_ID"),
            database_path=Path(os.getenv("DATABASE_PATH", "smartshell_max.db")).expanduser().resolve(),
            log_file=Path(os.getenv("LOG_FILE", "service.log")).expanduser().resolve(),
            log_level=log_level,
        )


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"Required environment variable {name} is missing")
    return value


def _required_url(name: str) -> str:
    value = _required(name).rstrip("/")
    if not value.startswith("https://"):
        raise ValueError(f"{name} must use HTTPS")
    return value


def _required_int(name: str) -> int:
    value = _required(name)
    try:
        return int(value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error


def _float_env(name: str, default: float) -> float:
    value = os.getenv(name, "").strip()
    if not value:
        return default
    try:
        return float(value)
    except ValueError as error:
        raise ValueError(f"{name} must be a number") from error


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name, "").strip().lower()
    if not value:
        return default
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


def _csv_list(name: str) -> list[str]:
    return [part.strip() for part in os.getenv(name, "").split(",") if part.strip()]


def _csv_set(name: str) -> set[str]:
    return {part.upper() for part in _csv_list(name)}
