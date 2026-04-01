"""Отправка email (OTP, уведомления)."""
from __future__ import annotations

import asyncio
import logging
import os
import smtplib
from email.message import EmailMessage

logger = logging.getLogger(__name__)


async def send_smtp_email(*, subject: str, body: str, to_addrs: list[str]) -> None:
    """Асинхронная обёртка над SMTP."""
    smtp_host = (os.getenv("SMTP_HOST") or "").strip()
    if not smtp_host:
        logger.warning("SMTP_HOST не задан — письмо не отправлено: %s", subject)
        logger.info("Текст (dev): %s\n%s", subject, body)
        return

    smtp_port = int(os.getenv("SMTP_PORT") or "587")
    smtp_user = (os.getenv("SMTP_USER") or "").strip()
    smtp_password = (os.getenv("SMTP_PASSWORD") or "").strip()
    smtp_from = (os.getenv("SMTP_FROM") or smtp_user or "no-reply@localhost").strip()
    use_tls = (os.getenv("SMTP_TLS", "1").strip() not in ("0", "false", "False"))

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = ", ".join(to_addrs)
    msg.set_content(body)

    def _send() -> None:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=25) as smtp:
            if use_tls:
                smtp.starttls()
            if smtp_user and smtp_password:
                smtp.login(smtp_user, smtp_password)
            smtp.send_message(msg)

    await asyncio.to_thread(_send)
