#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ежедневный пайплайн СПбМТСБ (пн–пт, МСК):

  1) с 13:50 опрашиваем сайт каждые 2 мин, пока не появится бюллетень на сегодня
  2) скачиваем PDF (xls — запасной вариант) в data/bulletins
  3) импортируем цены в БД
  4) рестартуем сервисы (чтобы веб/бот точно читали свежие цены)
  5) рассылаем таблицы из подписок (и опционально сводки по базису)

Примеры:
  python3 scripts/run_spimex_daily_pipeline.py --once
  python3 scripts/run_spimex_daily_pipeline.py --trade-date 2026-08-10 --once --no-restart --no-digest
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from import_spimex_prices_from_pdf import default_bulletins_directory, main_async
from utils.spimex_bulletin_fetch import (
    download_bulletin,
    find_bulletin_for_date,
    local_bulletin_for_date,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("spimex_daily")

_MSK = ZoneInfo("Europe/Moscow")
STATE_NAME = ".daily_pipeline_state.json"


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int((os.getenv(name, "") or str(default)).strip())
    except ValueError:
        return default


def _parse_hhmm(s: str, default: tuple[int, int]) -> tuple[int, int]:
    s = (s or "").strip()
    if not s or ":" not in s:
        return default
    a, b = s.split(":", 1)
    try:
        return int(a), int(b)
    except ValueError:
        return default


def _state_path(bulletins_dir: Path) -> Path:
    return bulletins_dir / STATE_NAME


def _load_state(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_state(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _already_done_today(state: dict, trade_date: date) -> bool:
    return str(state.get("last_success_trade_date") or "") == trade_date.isoformat()


def _wait_until_start(now: datetime, start_h: int, start_m: int) -> None:
    target = now.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
    if now >= target:
        return
    sec = (target - now).total_seconds()
    logger.info("Ждём начала окна %02d:%02d МСК (ещё %.0f сек)", start_h, start_m, sec)
    time.sleep(max(0.0, sec))


def _past_deadline(now: datetime, end_h: int, end_m: int) -> bool:
    end = now.replace(hour=end_h, minute=end_m, second=0, microsecond=0)
    return now >= end


def _restart_services(names: list[str]) -> None:
    names = [n.strip() for n in names if n.strip()]
    if not names:
        logger.info("Рестарт сервисов пропущен (список пуст)")
        return
    cmd = ["systemctl", "restart", *names]
    logger.info("Рестарт: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, timeout=120)
    except FileNotFoundError:
        logger.warning("systemctl не найден — рестарт пропущен")
    except subprocess.CalledProcessError as e:
        logger.error("systemctl restart failed: %s", e)
        # часто нужно sudo
        cmd2 = ["sudo", "-n", "systemctl", "restart", *names]
        logger.info("Пробую: %s", " ".join(cmd2))
        subprocess.run(cmd2, check=True, timeout=120)


async def _import_file(path: Path) -> None:
    await main_async(path, only_a_prefix=False, strict_market_only=False, log_extracted_codes=False)


async def _send_digests(*, include_basis: bool) -> dict[str, int]:
    from price_checker import send_digests_after_bulletin

    return await send_digests_after_bulletin(include_basis_digest=include_basis)


def run_pipeline(args: argparse.Namespace) -> int:
    now = datetime.now(_MSK)
    if now.weekday() >= 5 and not args.allow_weekend:
        logger.info("Выходной (%s) — бюллетеней нет, выход", now.strftime("%A"))
        return 0

    trade_date = args.trade_date or now.date()
    bulletins_dir = Path(args.bulletins_dir).expanduser().resolve() if args.bulletins_dir else default_bulletins_directory()
    state_path = _state_path(bulletins_dir)
    state = _load_state(state_path)

    if _already_done_today(state, trade_date) and not args.force:
        logger.info("На %s пайплайн уже успешно отработан — выход", trade_date.isoformat())
        return 0

    start_h, start_m = _parse_hhmm(os.getenv("SPIMEX_FETCH_START", "13:50"), (13, 50))
    end_h, end_m = _parse_hhmm(os.getenv("SPIMEX_FETCH_DEADLINE", "18:00"), (18, 0))
    interval = max(30, _env_int("SPIMEX_FETCH_INTERVAL_SEC", 120))

    if not args.once:
        _wait_until_start(datetime.now(_MSK), start_h, start_m)

    path: Path | None = None
    while True:
        now = datetime.now(_MSK)
        local = local_bulletin_for_date(bulletins_dir, trade_date)
        if local is not None:
            path = local
            logger.info("Локальный файл на %s: %s", trade_date.isoformat(), path.name)
            break

        link = find_bulletin_for_date(trade_date)
        if link is not None:
            path = download_bulletin(link, bulletins_dir)
            break

        if args.once:
            logger.error(
                "Бюллетень на %s ещё не появился или сайт недоступен с этой машины (--once). "
                "Проверьте: curl -4 -I https://spimex.com/ — если Connection refused, "
                "нужен HTTPS_PROXY/SOCKS или скачивание с другой машины + scp в data/bulletins.",
                trade_date.isoformat(),
            )
            return 2
        if _past_deadline(now, end_h, end_m):
            logger.error(
                "До %02d:%02d МСК бюллетень на %s не скачан (нет файла / сайт недоступен). "
                "С этой VPS до spimex.com:443 часто Connection refused — прокси или ручная загрузка.",
                end_h,
                end_m,
                trade_date.isoformat(),
            )
            return 3

        logger.info(
            "Бюллетеня на %s нет — следующая попытка через %s сек",
            trade_date.isoformat(),
            interval,
        )
        time.sleep(interval)

    assert path is not None
    logger.info("Импорт %s …", path)
    asyncio.run(_import_file(path))

    if not args.no_restart:
        services = (os.getenv("SPIMEX_RESTART_SERVICES") or "fuel-web fuel-max-bot").split()
        # небольшая пауза, чтобы sqlite успел сбросить WAL
        time.sleep(2)
        _restart_services(services)
        time.sleep(3)

    digest_counts = {"table": 0, "basis": 0}
    if not args.no_digest:
        include_basis = _env_bool("SPIMEX_DIGEST_INCLUDE_BASIS", True)
        if args.no_basis_digest:
            include_basis = False
        logger.info("Рассылка подписок после бюллетеня (basis=%s) …", include_basis)
        digest_counts = asyncio.run(_send_digests(include_basis=include_basis))
        logger.info("Отправлено: table=%s basis=%s", digest_counts.get("table"), digest_counts.get("basis"))

    state.update(
        {
            "last_success_trade_date": trade_date.isoformat(),
            "last_file": path.name,
            "last_success_at_msk": datetime.now(_MSK).isoformat(timespec="seconds"),
            "table_digest_sent": int(digest_counts.get("table") or 0),
            "basis_digest_sent": int(digest_counts.get("basis") or 0),
        }
    )
    _save_state(state_path, state)
    logger.info("Готово: %s", path.name)
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Скачать/импортировать бюллетень СПбМТСБ и разослать таблицы")
    ap.add_argument("--once", action="store_true", help="Одна попытка без ожидания 13:50 и без ретраев")
    ap.add_argument("--force", action="store_true", help="Игнорировать маркер «уже сделано сегодня»")
    ap.add_argument("--allow-weekend", action="store_true", help="Не пропускать сб/вс")
    ap.add_argument("--trade-date", type=date.fromisoformat, default=None, help="YYYY-MM-DD")
    ap.add_argument("--bulletins-dir", default=None, help="Каталог бюллетеней")
    ap.add_argument("--no-restart", action="store_true", help="Не рестартовать systemd-сервисы")
    ap.add_argument("--no-digest", action="store_true", help="Не слать подписки")
    ap.add_argument("--no-basis-digest", action="store_true", help="Слать только table_digest")
    args = ap.parse_args()
    raise SystemExit(run_pipeline(args))


if __name__ == "__main__":
    main()
