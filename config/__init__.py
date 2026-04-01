# -*- coding: utf-8 -*-
import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    """Настройки из переменных окружения / .env"""

    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "") or ""
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "sqlite+aiosqlite:///fuel_bot.db",
    )
    CHECK_INTERVAL_MINUTES: int = int(os.getenv("CHECK_INTERVAL_MINUTES", "60"))


config = Config()

__all__ = ["Config", "config"]
