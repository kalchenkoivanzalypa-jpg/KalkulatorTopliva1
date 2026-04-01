"""Настройки веб-приложения (переменные окружения)."""
from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

# Подпись cookie сессии и OTP
WEB_SECRET_KEY = os.getenv("WEB_SECRET_KEY", os.getenv("BOT_TOKEN", "change-me-in-production"))

# Админка сайта (отдельно от ADMIN_TELEGRAM_IDS бота)
ADMIN_WEB_PASSWORD = os.getenv("ADMIN_WEB_PASSWORD", "").strip()

# OTP
OTP_TTL_MINUTES = int(os.getenv("OTP_TTL_MINUTES", "15"))
OTP_MAX_ATTEMPTS = int(os.getenv("OTP_MAX_ATTEMPTS", "5"))

# Гостевой cookie для сохранения расчётов (synthetic telegram_id)
GUEST_COOKIE_NAME = "web_guest_tid"
