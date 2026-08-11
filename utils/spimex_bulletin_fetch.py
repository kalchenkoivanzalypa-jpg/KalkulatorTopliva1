#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Скачивание нефтяных бюллетеней СПбМТСБ (PDF приоритет, XLS если есть)."""
from __future__ import annotations

import http.client
import logging
import os
import re
import socket
import ssl
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date
from html import unescape
from pathlib import Path
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

SPIMEX_ORIGIN = "https://spimex.com"
RESULTS_URL = f"{SPIMEX_ORIGIN}/markets/oil_products/trades/results/"
UA = (
    "Mozilla/5.0 (compatible; fuel_bot/1.0; +https://calc.nk-vsnp.ru) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# oil_20260810162000.pdf / oil_xls_20260810162000.xls
_HREF_RE = re.compile(
    r'href=["\']([^"\']*?(?:oil_xls_|oil_)\d{14}\.(?:pdf|xls|xlsx))[^"\']*["\']',
    re.I,
)
_NAME_RE = re.compile(r"(oil_xls_|oil_)(\d{14})\.(pdf|xls|xlsx)", re.I)


def _force_ipv4() -> bool:
    # На VPS часто сломан IPv6 → Connection refused; по умолчанию только IPv4.
    return (os.getenv("SPIMEX_FORCE_IPV4") or "1").strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class BulletinLink:
    url: str
    filename: str
    trade_date: date
    kind: str  # pdf | xls

    @property
    def stamp14(self) -> str:
        m = _NAME_RE.search(self.filename)
        return m.group(2) if m else ""


def _ipv4_connect(host: str, port: int, *, timeout: float) -> socket.socket:
    last: Exception | None = None
    infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    if not infos:
        raise OSError(f"DNS: нет A-записи для {host}")
    logger.debug("DNS %s → %s", host, ", ".join(str(i[4][0]) for i in infos))
    for res in infos:
        af, socktype, proto, _canon, sockaddr = res
        sock = socket.socket(af, socktype, proto)
        try:
            sock.settimeout(timeout)
            sock.connect(sockaddr)
            return sock
        except OSError as e:
            last = e
            logger.warning("connect %s:%s (%s) failed: %s", host, port, sockaddr[0], e)
            try:
                sock.close()
            except OSError:
                pass
    raise OSError(f"IPv4 connect failed for {host}:{port}: {last}")


def _curl_get(url: str, *, timeout: float = 45.0, head: bool = False) -> tuple[int, bytes]:
    """Запасной канал: curl -4 (на части VPS urllib/OpenSSL режут, curl проходит)."""
    import subprocess

    cmd = [
        "curl",
        "-4",
        "-sS",
        "-L",
        "--connect-timeout",
        str(max(1, int(timeout // 3))),
        "--max-time",
        str(max(5, int(timeout))),
        "-A",
        UA,
        "-w",
        "\n__HTTP_CODE__:%{http_code}",
    ]
    if head:
        cmd.append("-I")
    cmd.append(url)
    try:
        p = subprocess.run(cmd, capture_output=True, timeout=timeout + 10)
    except FileNotFoundError as e:
        raise RuntimeError("curl не найден") from e
    if p.returncode != 0:
        err = (p.stderr or b"").decode("utf-8", "replace")[:300]
        raise RuntimeError(f"curl exit {p.returncode}: {err}")
    out = p.stdout or b""
    # отделяем код
    marker = b"\n__HTTP_CODE__:"
    idx = out.rfind(marker)
    if idx < 0:
        return 0, out
    body = out[:idx]
    try:
        code = int(out[idx + len(marker) :].strip() or "0")
    except ValueError:
        code = 0
    return code, body


def _http_backend() -> str:
    # auto | python | curl
    return (os.getenv("SPIMEX_HTTP_BACKEND") or "auto").strip().lower()


def _http_request(url: str, *, method: str = "GET", timeout: float = 45.0) -> tuple[int, bytes, dict[str, str]]:
    """
    HTTP(S) GET/HEAD. При SPIMEX_FORCE_IPV4=1 (дефолт) не ходим в IPv6.
    При ошибке python-стека — fallback на curl -4 (если backend=auto).
    """
    backend = _http_backend()
    method_u = method.upper()
    errors: list[str] = []

    def via_python() -> tuple[int, bytes, dict[str, str]]:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError(f"Unsupported URL scheme: {url}")
        host = parsed.hostname or ""
        if not host:
            raise ValueError(f"No host in URL: {url}")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"

        headers = {
            "User-Agent": UA,
            "Accept": "*/*",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            "Host": host,
            "Connection": "close",
        }

        if _force_ipv4():
            sock = _ipv4_connect(host, port, timeout=timeout)
            if parsed.scheme == "https":
                ctx = ssl.create_default_context()
                sock = ctx.wrap_socket(sock, server_hostname=host)
                conn: http.client.HTTPConnection = http.client.HTTPSConnection(
                    host, port=port, timeout=timeout, context=ctx
                )
                conn.sock = sock
            else:
                conn = http.client.HTTPConnection(host, port=port, timeout=timeout)
                conn.sock = sock
            try:
                conn.request(method_u, path, headers=headers)
                resp = conn.getresponse()
                body = resp.read() if method_u != "HEAD" else b""
                hdrs = {k.lower(): v for k, v in resp.getheaders()}
                return int(resp.status), body, hdrs
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        req = urllib.request.Request(url, headers=headers, method=method_u)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = int(getattr(resp, "status", None) or resp.getcode())
            body = resp.read() if method_u != "HEAD" else b""
            hdrs = {k.lower(): v for k, v in resp.headers.items()}
            return code, body, hdrs

    def via_curl() -> tuple[int, bytes, dict[str, str]]:
        code, body = _curl_get(url, timeout=timeout, head=(method_u == "HEAD"))
        return code, body, {}

    order: list[str]
    if backend == "curl":
        order = ["curl"]
    elif backend == "python":
        order = ["python"]
    else:
        order = ["python", "curl"]

    last_exc: Exception | None = None
    for name in order:
        try:
            if name == "python":
                return via_python()
            return via_curl()
        except Exception as e:
            last_exc = e
            errors.append(f"{name}: {e}")
            logger.warning("HTTP %s via %s failed: %s", method_u, name, e)

    raise RuntimeError(" / ".join(errors) if errors else f"HTTP failed: {last_exc}")


def _http_get(url: str, *, timeout: float = 45.0) -> bytes:
    code, body, _hdrs = _http_request(url, method="GET", timeout=timeout)
    if code >= 400:
        raise urllib.error.HTTPError(url, code, f"HTTP {code}", hdr=None, fp=None)  # type: ignore[arg-type]
    return body


def _http_head_ok(url: str, *, timeout: float = 20.0) -> bool:
    try:
        code, body, _hdrs = _http_request(url, method="HEAD", timeout=timeout)
        if 200 <= code < 300:
            return True
        if code in (403, 405, 501):
            code2, body2, _ = _http_request(url, method="GET", timeout=timeout)
            return 200 <= code2 < 300 and len(body2) > 1000
        return False
    except Exception:
        return False


def _parse_link(href: str) -> BulletinLink | None:
    href = unescape(href.strip())
    if not href:
        return None
    full = urljoin(SPIMEX_ORIGIN, href)
    path = urlparse(full).path
    name = Path(path).name
    m = _NAME_RE.search(name)
    if not m:
        return None
    stamp = m.group(2)
    ext = m.group(3).lower()
    trade = date(int(stamp[0:4]), int(stamp[4:6]), int(stamp[6:8]))
    kind = "pdf" if ext == "pdf" else "xls"
    clean = f"{SPIMEX_ORIGIN}{path}"
    return BulletinLink(url=clean, filename=name, trade_date=trade, kind=kind)


def list_bulletin_links_from_html(html: str) -> list[BulletinLink]:
    out: list[BulletinLink] = []
    seen: set[str] = set()
    for href in _HREF_RE.findall(html or ""):
        link = _parse_link(href)
        if not link or link.filename in seen:
            continue
        seen.add(link.filename)
        out.append(link)
    out.sort(key=lambda x: (x.trade_date, x.stamp14, 0 if x.kind == "pdf" else 1), reverse=True)
    return out


def fetch_results_page() -> str:
    raw = _http_get(RESULTS_URL)
    return raw.decode("utf-8", "replace")


def predicted_pdf_url(trade_date: date, *, hhmmss: str = "162000") -> str:
    stamp = f"{trade_date:%Y%m%d}{hhmmss}"
    return f"{SPIMEX_ORIGIN}/files/trades/result/pdf/oil/oil_{stamp}.pdf"


def find_bulletin_for_date(trade_date: date) -> BulletinLink | None:
    """
    Ищем бюллетень на дату торгов.
    Приоритет: PDF со страницы итогов → прямой URL oil_YYYYMMDD162000.pdf → XLS со страницы.
    """
    try:
        html = fetch_results_page()
        links = list_bulletin_links_from_html(html)
    except Exception:
        logger.exception("Не удалось прочитать страницу итогов СПбМТСБ")
        links = []

    pdfs = [x for x in links if x.kind == "pdf" and x.trade_date == trade_date]
    if pdfs:
        pdfs.sort(key=lambda x: x.stamp14, reverse=True)
        return pdfs[0]

    for hhmmss in ("162000", "140000", "135000", "170000", "160000"):
        url = predicted_pdf_url(trade_date, hhmmss=hhmmss)
        if _http_head_ok(url):
            name = Path(urlparse(url).path).name
            return BulletinLink(url=url, filename=name, trade_date=trade_date, kind="pdf")

    xls = [x for x in links if x.kind == "xls" and x.trade_date == trade_date]
    if xls:
        xls.sort(key=lambda x: x.stamp14, reverse=True)
        return xls[0]
    return None


def download_bulletin(link: BulletinLink, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / link.filename
    if dest.is_file() and dest.stat().st_size > 10_000:
        logger.info("Бюллетень уже на диске: %s (%s байт)", dest.name, dest.stat().st_size)
        return dest

    logger.info("Скачиваю %s → %s", link.url, dest)
    data = _http_get(link.url)
    if len(data) < 10_000:
        raise RuntimeError(f"Слишком маленький ответ ({len(data)} байт) для {link.url}")
    if link.kind == "pdf" and not data.startswith(b"%PDF"):
        raise RuntimeError(f"Ответ не похож на PDF: {link.url}")
    tmp = dest.with_suffix(dest.suffix + ".part")
    tmp.write_bytes(data)
    tmp.replace(dest)
    logger.info("Сохранено %s (%s байт)", dest.name, dest.stat().st_size)
    return dest


def local_bulletin_for_date(dest_dir: Path, trade_date: date) -> Path | None:
    if not dest_dir.is_dir():
        return None
    candidates: list[Path] = []
    for p in dest_dir.iterdir():
        if not p.is_file():
            continue
        m = _NAME_RE.search(p.name)
        if not m:
            continue
        stamp = m.group(2)
        d = date(int(stamp[0:4]), int(stamp[4:6]), int(stamp[6:8]))
        if d == trade_date:
            candidates.append(p)
    if not candidates:
        return None

    def key(p: Path) -> tuple:
        m = _NAME_RE.search(p.name)
        stamp = m.group(2) if m else ""
        ext = p.suffix.lower()
        return (0 if ext == ".pdf" else 1, stamp)

    return max(candidates, key=key)


__all__ = [
    "BulletinLink",
    "RESULTS_URL",
    "download_bulletin",
    "find_bulletin_for_date",
    "local_bulletin_for_date",
    "list_bulletin_links_from_html",
]
