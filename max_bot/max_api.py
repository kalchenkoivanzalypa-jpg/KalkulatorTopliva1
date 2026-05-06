from __future__ import annotations

import os
from typing import Any, Optional

import httpx


class MaxApiError(RuntimeError):
    pass


class MaxApi:
    """
    Minimal MAX API client.

    Docs:
    - https://dev.max.ru/docs-api/methods/POST/messages
    - https://dev.max.ru/docs-api/methods/POST/answers
    - https://dev.max.ru/docs-api/methods/POST/subscriptions
    """

    def __init__(self, token: str, *, base_url: str = "https://platform-api.max.ru") -> None:
        self._token = (token or "").strip()
        if not self._token:
            raise MaxApiError("MAX_BOT_TOKEN is missing")
        self._base_url = base_url.rstrip("/")

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": self._token,
            "Content-Type": "application/json",
        }

    async def send_message(
        self,
        *,
        user_id: Optional[int] = None,
        chat_id: Optional[int] = None,
        text: str,
        attachments: Optional[list[dict[str, Any]]] = None,
        fmt: Optional[str] = "html",
        notify: bool = True,
    ) -> dict[str, Any]:
        if not user_id and not chat_id:
            raise MaxApiError("send_message requires user_id or chat_id")
        params: dict[str, Any] = {}
        if user_id:
            params["user_id"] = int(user_id)
        if chat_id:
            params["chat_id"] = int(chat_id)
        body: dict[str, Any] = {"text": text, "notify": bool(notify)}
        if fmt:
            body["format"] = fmt
        if attachments is not None:
            body["attachments"] = attachments
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                f"{self._base_url}/messages",
                params=params,
                headers=self._headers(),
                json=body,
            )
        if r.status_code >= 400:
            raise MaxApiError(f"MAX /messages failed: {r.status_code} {r.text}")
        return r.json()

    async def answer_callback(
        self,
        *,
        callback_id: str,
        notification: Optional[str] = None,
        message: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        cid = (callback_id or "").strip()
        if not cid:
            raise MaxApiError("callback_id is required")
        body: dict[str, Any] = {}
        if notification is not None:
            body["notification"] = str(notification)
        if message is not None:
            body["message"] = message
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                f"{self._base_url}/answers",
                params={"callback_id": cid},
                headers=self._headers(),
                json=body,
            )
        if r.status_code >= 400:
            raise MaxApiError(f"MAX /answers failed: {r.status_code} {r.text}")
        return r.json()

    async def subscribe_webhook(
        self,
        *,
        url: str,
        update_types: list[str],
        secret: Optional[str] = None,
    ) -> dict[str, Any]:
        u = (url or "").strip()
        if not u.startswith("https://"):
            raise MaxApiError("Webhook url must start with https://")
        body: dict[str, Any] = {"url": u, "update_types": list(update_types or [])}
        if secret:
            body["secret"] = str(secret)
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.post(
                f"{self._base_url}/subscriptions",
                headers=self._headers(),
                json=body,
            )
        if r.status_code >= 400:
            raise MaxApiError(f"MAX /subscriptions failed: {r.status_code} {r.text}")
        return r.json()


def max_api_from_env() -> MaxApi:
    return MaxApi(os.getenv("MAX_BOT_TOKEN", "") or "")

