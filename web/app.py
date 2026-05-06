"""FastAPI: веб-версия калькулятора и кабинета."""
from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.requests import Request
from starlette.responses import Response, PlainTextResponse

from config import config
from db.database import init_db
from price_checker import start_price_checker
from web import settings
from web.routes_admin import router as admin_router
from web.routes_analytics import router as analytics_router
from web.routes_auth import router as auth_router
from web.routes_calc import router as calc_router
from web.routes_cabinet import router as cabinet_router
from web.routes_pages import router as pages_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

class _SlidingWindowLimiter:
    def __init__(self) -> None:
        # key -> list[timestamps]
        self._hits: dict[str, list[float]] = {}

    def allow(self, key: str, *, limit: int, window_sec: int) -> bool:
        now = time.time()
        xs = self._hits.get(key)
        if xs is None:
            self._hits[key] = [now]
            return True
        cutoff = now - float(window_sec)
        # drop old
        xs[:] = [t for t in xs if t >= cutoff]
        if len(xs) >= int(limit):
            return False
        xs.append(now)
        return True


class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Lightweight rate limit for OTP/login endpoints.
    Note: per-worker memory only (ok for brute/spam reduction).
    """

    def __init__(self, app: FastAPI) -> None:
        super().__init__(app)
        self.lim = _SlidingWindowLimiter()

    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path
        method = request.method.upper()
        if method == "POST" and path in ("/login", "/verify", "/admin/login"):
            ip = (request.headers.get("x-forwarded-for") or request.client.host or "unknown").split(",")[0].strip()
            # IP-level limits
            if path == "/login":
                # sending OTP: 6 per 10 minutes per IP
                if not self.lim.allow(f"login_ip:{ip}", limit=6, window_sec=600):
                    return PlainTextResponse("Too many requests", status_code=429)
            elif path == "/verify":
                # verifying OTP: 20 per 10 minutes per IP
                if not self.lim.allow(f"verify_ip:{ip}", limit=20, window_sec=600):
                    return PlainTextResponse("Too many requests", status_code=429)
            elif path == "/admin/login":
                if not self.lim.allow(f"admin_ip:{ip}", limit=10, window_sec=600):
                    return PlainTextResponse("Too many requests", status_code=429)

        return await call_next(request)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    logger.info("БД готова")

    bot: Bot | None = None
    token = (config.BOT_TOKEN or "").strip()
    if token:
        bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        app.state.bot = bot
    else:
        app.state.bot = None
        logger.warning("BOT_TOKEN не задан — уведомления только по email (если настроен SMTP)")

    # IMPORTANT:
    # fuel-web обычно работает несколькими воркерами uvicorn/gunicorn.
    # Фоновые циклы подписок/дайджеста должны работать в одном экземпляре (отдельный сервис),
    # иначе получаются дубли (например 3 одинаковых уведомления при workers=3).
    if os.getenv("DISABLE_WEB_PRICE_CHECKER", "").strip().lower() not in ("1", "true", "yes"):
        await start_price_checker(bot)
        logger.info("Фоновые проверки цен запущены")
    else:
        logger.info("Фоновые проверки цен выключены в fuel-web (DISABLE_WEB_PRICE_CHECKER=1)")

    yield

    if bot is not None:
        await bot.session.close()


app = FastAPI(title="НК калькулятор топлива", lifespan=lifespan)

app.add_middleware(RateLimitMiddleware)
app.add_middleware(
    SessionMiddleware,
    secret_key=settings.WEB_SECRET_KEY,
    session_cookie="fuel_web_session",
    max_age=86400 * 30,
    same_site="lax",
)

app.mount("/static", StaticFiles(directory="web/static"), name="static")

app.include_router(pages_router)
app.include_router(calc_router)
app.include_router(analytics_router)
app.include_router(auth_router)
app.include_router(cabinet_router)
app.include_router(admin_router)
