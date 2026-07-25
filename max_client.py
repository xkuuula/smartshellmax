from __future__ import annotations

import asyncio
import email.utils
import html
from html.parser import HTMLParser
import logging
import ssl
import time
from dataclasses import dataclass
from typing import Any

import aiohttp
import certifi

from config import Config


logger = logging.getLogger(__name__)
MAX_TEXT_LIMIT = 4000


class MaxApiError(Exception):
    def __init__(
        self,
        message: str,
        *,
        transient: bool,
        uncertain: bool = False,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.transient = transient
        self.uncertain = uncertain
        self.retry_after = retry_after


@dataclass(frozen=True, slots=True)
class MaxMessage:
    message_id: str | None


class MaxClient:
    def __init__(self, config: Config) -> None:
        self._base_url = config.max_api_base
        self._token = config.max_bot_token
        self._target_chat_id = config.max_target_chat_id
        self._ssl_verify = config.max_ssl_verify
        self._session: aiohttp.ClientSession | None = None
        self._bot_user_id: int | None = None

    async def __aenter__(self) -> "MaxClient":
        if self._ssl_verify:
            ssl_context: ssl.SSLContext | bool = ssl.create_default_context(cafile=certifi.where())
            logger.info("MAX client SSL verification enabled, CA bundle=%s", certifi.where())
        else:
            ssl_context = False
            logger.warning("MAX client SSL verification is disabled by MAX_SSL_VERIFY=false")
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        timeout = aiohttp.ClientTimeout(total=35, connect=10, sock_read=30)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={"Authorization": self._token, "Accept": "application/json"},
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def send_text(self, text: str) -> MaxMessage:
        payload = {"text": to_max_html(text), "format": "html", "notify": True}
        response = await self._request(
            "POST",
            "/messages",
            params={
                "chat_id": str(self._target_chat_id),
                "disable_link_preview": "true",
            },
            json=payload,
            sending=True,
        )
        return MaxMessage(message_id=_extract_message_id(response))

    async def find_matching_message(
        self,
        plain_text: str,
        attempt_started_at: float,
        claimed_ids: set[str],
    ) -> str | None:
        bot_user_id = await self._get_bot_user_id()
        expected_html = to_max_html(plain_text)
        payload = await self._request(
            "GET",
            "/messages",
            params={"chat_id": str(self._target_chat_id), "count": "100"},
        )
        messages = payload.get("messages", []) if isinstance(payload, dict) else []
        if not isinstance(messages, list):
            return None

        for message in messages:
            if not isinstance(message, dict):
                continue
            message_id = _extract_message_id(message)
            if message_id is None or message_id in claimed_ids:
                continue
            sender_id = _extract_sender_id(message)
            if sender_id != bot_user_id:
                continue
            timestamp = _extract_timestamp(message)
            if timestamp is None or timestamp < attempt_started_at - 15:
                continue
            body = message.get("body")
            body_text = body.get("text") if isinstance(body, dict) else message.get("text")
            if body_text in {plain_text, expected_html}:
                return message_id
        return None

    async def _get_bot_user_id(self) -> int:
        if self._bot_user_id is None:
            payload = await self._request("GET", "/me")
            try:
                self._bot_user_id = int(payload["user_id"])
            except (KeyError, TypeError, ValueError) as error:
                raise MaxApiError(
                    "MAX /me response does not contain user_id",
                    transient=True,
                ) from error
        return self._bot_user_id

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        sending: bool = False,
    ) -> Any:
        session = self._require_session()
        url = f"{self._base_url}{path}"
        try:
            async with session.request(method, url, params=params, json=json) as response:
                raw_text = await response.text()
                if 200 <= response.status < 300:
                    if not raw_text:
                        return {}
                    try:
                        return await response.json(content_type=None)
                    except (ValueError, aiohttp.ContentTypeError) as error:
                        raise MaxApiError(
                            f"MAX returned invalid JSON for {method} {path}",
                            transient=True,
                            uncertain=sending,
                        ) from error

                retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                message = f"MAX API {method} {path} returned HTTP {response.status}: {_compact(raw_text)}"
                if response.status in {408, 409, 425, 429} or response.status >= 500:
                    raise MaxApiError(
                        message,
                        transient=True,
                        uncertain=sending,
                        retry_after=retry_after,
                    )
                raise MaxApiError(message, transient=False)
        except MaxApiError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as error:
            raise MaxApiError(
                f"MAX API network error during {method} {path}: {error}",
                transient=True,
                uncertain=sending,
            ) from error

    def _require_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("MAX client is not open")
        return self._session


def split_for_max(text: str, limit: int = MAX_TEXT_LIMIT) -> list[str]:
    if not text:
        return []

    parts: list[str] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + limit)
        if end < len(text):
            newline = text.rfind("\n", start, end)
            space = text.rfind(" ", start, end)
            natural = max(newline, space)
            if natural > start + int(limit * 0.65):
                end = natural + 1
        parts.append(text[start:end])
        start = end
    return parts


def to_max_html(text: str) -> str:
    parser = _MaxHtmlSanitizer()
    parser.feed(text)
    parser.close()
    return parser.result()


def _extract_message_id(payload: dict[str, Any]) -> str | None:
    candidates: list[Any] = [payload.get("message_id"), payload.get("mid")]
    message = payload.get("message")
    if isinstance(message, dict):
        candidates.extend([message.get("message_id"), message.get("mid")])
        body = message.get("body")
        if isinstance(body, dict):
            candidates.extend([body.get("mid"), body.get("message_id")])
    body = payload.get("body")
    if isinstance(body, dict):
        candidates.extend([body.get("mid"), body.get("message_id")])
    for candidate in candidates:
        if candidate is not None:
            return str(candidate)
    return None


def _extract_sender_id(message: dict[str, Any]) -> int | None:
    sender = message.get("sender")
    candidates: list[Any] = []
    if isinstance(sender, dict):
        candidates.extend([sender.get("user_id"), sender.get("userId"), sender.get("id")])
    candidates.extend([message.get("sender_id"), message.get("senderId")])
    for candidate in candidates:
        try:
            if candidate is not None:
                return int(candidate)
        except (TypeError, ValueError):
            continue
    return None


def _extract_timestamp(message: dict[str, Any]) -> float | None:
    body = message.get("body")
    candidates = [message.get("timestamp"), message.get("created_at")]
    if isinstance(body, dict):
        candidates.extend([body.get("timestamp"), body.get("created_at")])
    for candidate in candidates:
        try:
            value = float(candidate)
        except (TypeError, ValueError):
            continue
        if value > 10_000_000_000:
            value /= 1000
        return value
    return None


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            parsed = email.utils.parsedate_to_datetime(value)
            return max(0.0, parsed.timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return None


def _compact(value: str) -> str:
    return " ".join(value.split())[:800] if value else "empty response"


class _MaxHtmlSanitizer(HTMLParser):
    _simple_tags = {"b", "strong", "i", "em", "u", "s", "strike", "del", "code", "pre"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._chunks: list[str] = []
        self._open_tags: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag == "br":
            self._chunks.append("\n")
            return
        if tag in self._simple_tags:
            normalized = _normalize_tag(tag)
            self._chunks.append(f"<{normalized}>")
            self._open_tags.append(normalized)
            return
        if tag == "a":
            href = _attr(attrs, "href")
            if href and href.startswith(("http://", "https://", "mailto:")):
                self._chunks.append(f'<a href="{html.escape(href, quote=True)}">')
                self._open_tags.append("a")

    def handle_endtag(self, tag: str) -> None:
        normalized = _normalize_tag(tag.lower())
        for index in range(len(self._open_tags) - 1, -1, -1):
            if self._open_tags[index] == normalized:
                for closing in reversed(self._open_tags[index:]):
                    self._chunks.append(f"</{closing}>")
                del self._open_tags[index:]
                return

    def handle_data(self, data: str) -> None:
        self._chunks.append(html.escape(data))

    def result(self) -> str:
        for tag in reversed(self._open_tags):
            self._chunks.append(f"</{tag}>")
        self._open_tags.clear()
        return "".join(self._chunks)


def _normalize_tag(tag: str) -> str:
    if tag == "strong":
        return "b"
    if tag == "em":
        return "i"
    if tag in {"strike", "del"}:
        return "s"
    return tag


def _attr(attrs: list[tuple[str, str | None]], name: str) -> str | None:
    for key, value in attrs:
        if key.lower() == name and value:
            return value
    return None
