# -*- coding: utf-8 -*-
"""Точка входа Telegram-бота расчёта ГСМ."""

import asyncio
import logging
import os

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

from bot.handlers import router
from bot.analytics_handlers import analytics_router
from bot.admin_handlers import admin_router
from config import config
from db.database import init_db
from price_checker import start_price_checker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def _maybe_import_latest_spimex_bulletin() -> None:
    """Если SPIMEX_IMPORT_ON_STARTUP=1 — цены из data/bulletins (новейший PDF по дате в имени)."""
    if os.getenv("SPIMEX_IMPORT_ON_STARTUP", "").strip().lower() not in (
        "1",
        "true",
        "yes",
    ):
        return
    try:
        import import_spimex_prices_from_pdf as spx
    except Exception as e:
        logger.warning("СПбМТСБ при старте: не удалось импортировать модуль: %s", e)
        return
    try:
        d = spx.default_bulletins_directory()
        pdf = spx.pick_latest_bulletin_pdf(d)
        pairs = spx.extract_market_prices_from_pdf(
            pdf,
            only_a_prefix=False,
            strict_market_only=False,
        )
        up, miss, _ = await spx.apply_prices(pairs)
        logger.info(
            "✅ СПбМТСБ при старте: %s → обновлено цен в БД: %s (кодов в PDF: %s, не в каталоге: %s)",
            pdf.name,
            up,
            len(pairs),
            miss,
        )
    except FileNotFoundError as e:
        logger.warning("СПбМТСБ при старте пропущен: %s", e)
    except Exception:
        logger.exception("Ошибка импорта СПбМТСБ при старте")


async def on_startup(bot: Bot):
    logger.info("🚀 Бот запускается...")
    
    await init_db()
    logger.info("✅ База данных готова")

    await _maybe_import_latest_spimex_bulletin()
    
    await start_price_checker(bot)
    logger.info("✅ Система проверки цен запущена")


async def on_shutdown(bot: Bot):
    logger.info("🛑 Бот останавливается...")
    
    if hasattr(bot, 'data') and 'scheduler' in bot.data:
        bot.data['scheduler'].shutdown()
        logger.info("✅ Планировщик подписок остановлен")


async def main():
    logger.info("=" * 50)
    logger.info("ЗАПУСК БОТА РАСЧЕТА ГСМ")
    logger.info("=" * 50)
    
    if not config.BOT_TOKEN:
        logger.error("❌ ОШИБКА: BOT_TOKEN не найден в .env файле!")
        logger.error("Создайте файл .env и добавьте строку: BOT_TOKEN=ваш_токен_от_BotFather")
        return
    
    bot = Bot(
        token=config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    
    dp = Dispatcher(storage=MemoryStorage())
    
    # Важно: admin_router подключаем первым, чтобы /admin не перехватывался
    # state-хендлерами обычных текстовых сценариев.
    dp.include_router(admin_router)
    dp.include_router(router)
    dp.include_router(analytics_router)
    
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    try:
        logger.info("✅ Бот готов к работе. Нажмите Ctrl+C для остановки.")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске: {e}")
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())