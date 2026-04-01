# add_transport_type.py
import asyncio
import logging
from db.database import Basis, get_session
from sqlalchemy import Column, String, text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def add_transport_type_column():
    """Добавление колонки transport_type в таблицу basis"""
    session = await get_session()
    try:
        # Проверяем, существует ли колонка
        result = await session.execute(
            text("PRAGMA table_info(basis)")
        )
        columns = result.fetchall()
        column_names = [col[1] for col in columns]
        
        if 'transport_type' not in column_names:
            logger.info("Добавление колонки transport_type в таблицу basis...")
            await session.execute(
                text("ALTER TABLE basis ADD COLUMN transport_type VARCHAR(10) DEFAULT 'rail'")
            )
            await session.commit()
            logger.info("✅ Колонка transport_type успешно добавлена")
            
            # Обновляем существующие записи
            await session.execute(
                text("UPDATE basis SET transport_type = 'rail' WHERE transport_type IS NULL")
            )
            await session.commit()
            logger.info("✅ Существующие записи обновлены")
        else:
            logger.info("Колонка transport_type уже существует")
            
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
    finally:
        await session.close()

if __name__ == "__main__":
    asyncio.run(add_transport_type_column())