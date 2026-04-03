"""Отправка email (OTP, уведомления)."""
from __future__ import annotations

import asyncio
import logging
import os
import smtplib
import socket
import ssl
from email.message import EmailMessage
from typing import Any

logger = logging.getLogger(__name__)


class SMTPNotConfiguredError(Exception):
    """В .env не задан SMTP_HOST (ожидается для OTP и обязательной отправки)."""


def smtp_is_configured() -> bool:
    return bool((os.getenv("SMTP_HOST") or "").strip())


def _truthy(val: str | None) -> bool:
    if val is None:
        return False
    return val.strip().lower() in ("1", "true", "yes", "on")


def smtp_connection_mode() -> str:
    """Краткое описание режима для админки: ssl | starttls | plain."""
    port = int(os.getenv("SMTP_PORT") or "587")
    if _truthy(os.getenv("SMTP_SSL")) or port == 465:
        return "ssl"
    if _truthy(os.getenv("SMTP_TLS", "1")):
        return "starttls"
    return "plain"


def smtp_status_for_admin() -> dict[str, Any]:
    """Маскированные поля для страницы /admin (без пароля)."""
    host = (os.getenv("SMTP_HOST") or "").strip()
    if len(host) > 6:
        host_hint = host[:2] + "…" + host[-2:]
    elif host:
        host_hint = host[0] + "…"
    else:
        host_hint = "—"
    user = (os.getenv("SMTP_USER") or "").strip()
    if "@" in user:
        u, _, d = user.partition("@")
        user_hint = (u[:1] + "…@" + d) if u else user
    elif len(user) > 2:
        user_hint = user[:2] + "…"
    else:
        user_hint = user or "—"
    return {
        "configured": smtp_is_configured(),
        "host_hint": host_hint,
        "port": int(os.getenv("SMTP_PORT") or "587"),
        "mode": smtp_connection_mode(),
        "user_hint": user_hint,
        "from_addr": (os.getenv("SMTP_FROM") or "").strip() or "—",
    }


def _smtp_timeout_sec() -> float:
    try:
        return float((os.getenv("SMTP_TIMEOUT") or "20").strip())
    except ValueError:
        return 20.0


def smtp_force_ipv4() -> bool:
    """
    На многих VPS IPv6 «есть» в DNS, но маршрута нет → OSError 101 Network is unreachable.
    По умолчанию шлём SMTP только по IPv4.
    Отключить: SMTP_FORCE_IPV4=0
    """
    return (os.getenv("SMTP_FORCE_IPV4") or "1").strip().lower() in ("1", "true", "yes", "on")


def _tcp_connect_ipv4(
    host: str,
    port: int,
    timeout: float | None,
    source_address: tuple[str, int] | None,
) -> socket.socket:
    """Только AF_INET — обходит сломанный IPv6 на VPS."""
    if timeout is not None and not timeout:
        raise ValueError("Non-blocking socket (timeout=0) is not supported")
    last: OSError | None = None
    for res in socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM):
        af, socktype, proto, _canon, sa = res
        sock: socket.socket | None = None
        try:
            sock = socket.socket(af, socktype, proto)
            sock.settimeout(timeout)
            if source_address is not None:
                sock.bind(source_address)
            sock.connect(sa)
            return sock
        except OSError as e:
            last = e
            if sock is not None:
                sock.close()
    if last is not None:
        raise last
    raise OSError(f"SMTP: нет IPv4 для {host!r}:{port}")


class _SMTP_INET(smtplib.SMTP):
    def _get_socket(self, host, port, timeout):
        if smtp_force_ipv4():
            return _tcp_connect_ipv4(host, port, timeout, self.source_address)
        return super()._get_socket(host, port, timeout)


class _SMTP_SSL_INET(smtplib.SMTP_SSL):
    def _get_socket(self, host, port, timeout):
        if self.debuglevel > 0:
            self._print_debug("connect:", (host, port))
        if smtp_force_ipv4():
            new_socket = _tcp_connect_ipv4(host, port, timeout, self.source_address)
        else:
            new_socket = smtplib.SMTP._get_socket(self, host, port, timeout)
        return self.context.wrap_socket(new_socket, server_hostname=self._host)


def _send_sync(msg: EmailMessage) -> None:
    smtp_host = (os.getenv("SMTP_HOST") or "").strip()
    smtp_port = int(os.getenv("SMTP_PORT") or "587")
    smtp_user = (os.getenv("SMTP_USER") or "").strip()
    smtp_password = (os.getenv("SMTP_PASSWORD") or "").strip()
    use_tls = _truthy(os.getenv("SMTP_TLS", "1"))
    # Явный SSL (часто порт 465: Яндекс, Mail.ru) или SMTP_SSL=1
    use_ssl = _truthy(os.getenv("SMTP_SSL")) or smtp_port == 465
    ctx = ssl.create_default_context()
    timeout = _smtp_timeout_sec()

    if use_ssl:
        with _SMTP_SSL_INET(smtp_host, smtp_port, timeout=timeout, context=ctx) as smtp:
            if smtp_user and smtp_password:
                smtp.login(smtp_user, smtp_password)
            smtp.send_message(msg)
        return

    with _SMTP_INET(smtp_host, smtp_port, timeout=timeout) as smtp:
        if use_tls:
            smtp.starttls(context=ctx)
        if smtp_user and smtp_password:
            smtp.login(smtp_user, smtp_password)
        smtp.send_message(msg)


async def send_smtp_email(
    *,
    subject: str,
    body: str,
    to_addrs: list[str],
    require_smtp: bool = False,
) -> None:
    """
    Асинхронная обёртка над SMTP.
    Если require_smtp=True и SMTP_HOST пуст — SMTPNotConfiguredError.
    При ошибке соединения/логина при require_smtp — пробрасывается исключение.
    Без require_smtp при пустом хосте только логируем (как раньше).
    """
    to_addrs = [a.strip() for a in to_addrs if (a or "").strip()]
    if not to_addrs:
        if require_smtp:
            raise ValueError("Не указаны получатели письма")
        logger.warning("send_smtp_email: пустой список получателей — %s", subject)
        return

    smtp_host = (os.getenv("SMTP_HOST") or "").strip()
    if not smtp_host:
        if require_smtp:
            raise SMTPNotConfiguredError("SMTP_HOST не задан в окружении сервера")
        logger.warning("SMTP_HOST не задан — письмо не отправлено: %s", subject)
        logger.info("Текст (dev): %s\n%s", subject, body)
        return

    smtp_from = (os.getenv("SMTP_FROM") or os.getenv("SMTP_USER") or "no-reply@localhost").strip()

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = ", ".join(to_addrs)
    msg.set_content(body, charset="utf-8")

    try:
        await asyncio.to_thread(_send_sync, msg)
    except Exception:
        if require_smtp:
            raise
        logger.exception("Ошибка SMTP (письмо не отправлено): %s", subject)
