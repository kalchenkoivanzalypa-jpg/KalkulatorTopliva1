"""FastAPI: веб-версия калькулятора и кабинета."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

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

    await start_price_checker(bot)
    logger.info("Фоновые проверки цен запущены")

    yield

    if bot is not None:
        await bot.session.close()


app = FastAPI(title="НК калькулятор топлива", lifespan=lifespan)

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
