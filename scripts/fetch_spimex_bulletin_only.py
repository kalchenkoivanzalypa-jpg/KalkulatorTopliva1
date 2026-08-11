#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Только скачивание бюллетеня СПбМТСБ (без БД) — для GitHub Actions / Mac.

  python3 scripts/fetch_spimex_bulletin_only.py --out-dir ./_bulletin --once
  python3 scripts/fetch_spimex_bulletin_only.py --out-dir ./_bulletin   # ретраи до дедлайна
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.spimex_bulletin_fetch import (
    download_bulletin,
    find_bulletin_for_date,
    local_bulletin_for_date,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("spimex_fetch_only")
_MSK = ZoneInfo("Europe/Moscow")


def _parse_hhmm(s: str, default: tuple[int, int]) -> tuple[int, int]:
    s = (s or "").strip()
    if not s or ":" not in s:
        return default
    a, b = s.split(":", 1)
    try:
        return int(a), int(b)
    except ValueError:
        return default


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--trade-date", type=date.fromisoformat, default=None)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--allow-weekend", action="store_true")
    ap.add_argument(
        "--interval-sec",
        type=int,
        default=int(os.getenv("SPIMEX_FETCH_INTERVAL_SEC", "120") or "120"),
    )
    args = ap.parse_args()

    # На GitHub Actions / Mac IPv6 обычно ок; curl-fallback всё равно есть
    os.environ.setdefault("SPIMEX_FORCE_IPV4", "0")
    os.environ.setdefault("SPIMEX_HTTP_BACKEND", "auto")

    now = datetime.now(_MSK)
    if now.weekday() >= 5 and not args.allow_weekend:
        logger.info("Выходной — выход 0")
        return 0

    trade_date = args.trade_date or now.date()
    out_dir = args.out_dir.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    end_h, end_m = _parse_hhmm(os.getenv("SPIMEX_FETCH_DEADLINE", "18:00"), (18, 0))
    interval = max(30, int(args.interval_sec))

    while True:
        now = datetime.now(_MSK)
        local = local_bulletin_for_date(out_dir, trade_date)
        if local is not None and local.stat().st_size > 10_000:
            logger.info("Уже есть: %s", local)
            print(local)
            return 0

        link = find_bulletin_for_date(trade_date)
        if link is not None:
            path = download_bulletin(link, out_dir)
            print(path)
            return 0

        if args.once:
            logger.error("Бюллетень на %s ещё не доступен", trade_date.isoformat())
            return 2

        deadline = now.replace(hour=end_h, minute=end_m, second=0, microsecond=0)
        if now >= deadline:
            logger.error("Дедлайн %02d:%02d МСК — бюллетень на %s не появился", end_h, end_m, trade_date)
            return 3

        logger.info("Ждём бюллетень на %s, sleep %ss", trade_date.isoformat(), interval)
        time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
