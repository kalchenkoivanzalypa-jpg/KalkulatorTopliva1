#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standalone runner for background subscriptions/daily digest.

Run as a single process under systemd to avoid duplicate notifications when fuel-web
is started with multiple uvicorn workers.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

ROOT = str(Path(__file__).resolve().parents[1])
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from db.database import init_db
from price_checker import start_price_checker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    await init_db()
    logger.info("✅ БД готова (price-checker)")
    # bot=None is OK: notifications go to MAX (max_user_id) or email.
    await start_price_checker(bot=None)
    logger.info("✅ price-checker loops started")
    # Keep process alive forever (start_price_checker schedules background tasks and returns)
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())

