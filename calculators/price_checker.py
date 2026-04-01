import asyncio
import logging
from datetime import datetime
from sqlalchemy import select, update
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from db.database import NotificationLog, PriceAlert, Product, User, get_session
from config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def check_price_drops():
    """
    Проверка снижения цен и отправка уведомлений
    Запускается по расписанию
    """
    logger.info("⏰ Проверка цен для подписок...")
    
    session = await get_session()
    try:
        result = await session.execute(
            select(PriceAlert)
            .where(PriceAlert.is_active == True)
            .where(PriceAlert.triggered_at == None)
        )
        alerts = result.scalars().all()
        
        if not alerts:
            logger.info("Нет активных подписок")
            return
        
        logger.info(f"Найдено активных подписок: {len(alerts)}")
        
        for alert in alerts:
            try:
                product = await session.get(Product, alert.product_id)
                if not product:
                    logger.warning(f"Продукт {alert.product_id} не найден")
                    continue
                
                if product.current_price <= alert.target_price:
                    logger.info(f"🎯 Цена упала! Подписка #{alert.id}")
                    
                    user = await session.get(User, alert.user_id)
                    if not user:
                        logger.warning(f"Пользователь {alert.user_id} не найден")
                        continue
                    
                    logger.info(f"Уведомление для пользователя {user.telegram_id}: "
                               f"Цена на {product.name} упала до {product.current_price}")
                    
                    alert.triggered_at = datetime.now()
                    alert.notification_sent = True
                    alert.is_active = False
                    
                    log = NotificationLog(
                        user_id=user.id,
                        alert_id=alert.id,
                        notification_type='telegram',
                        message=f"Price dropped to {product.current_price}",
                        is_success=True
                    )
                    session.add(log)
                    
                    await session.commit()
                    logger.info(f"✅ Подписка #{alert.id} обработана")
            except Exception as e:
                logger.error(f"Ошибка при обработке подписки #{alert.id}: {e}")
                continue
    except Exception as e:
        logger.error(f"Ошибка при проверке цен: {e}")
    finally:
        await session.close()
        logger.info("Сессия БД закрыта")


async def start_price_checker(bot):
    """
    Запуск планировщика проверки цен
    """
    scheduler = AsyncIOScheduler()
    
    # Проверка подписок каждый час
    scheduler.add_job(
        check_price_drops,
        'interval',
        minutes=config.CHECK_INTERVAL_MINUTES,
        id='price_checker',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info(f"✅ Планировщик запущен. Интервал: {config.CHECK_INTERVAL_MINUTES} минут")
    
    bot.data = {'scheduler': scheduler}