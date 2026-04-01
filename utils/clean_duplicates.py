# clean_duplicates.py
import asyncio
import logging
from db.database import CityDestination, get_session
from sqlalchemy import select, delete

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def clean_duplicate_cities():
    """Удаление дубликатов городов"""
    session = await get_session()
    try:
        # Получаем все города
        result = await session.execute(
            select(CityDestination)
        )
        cities = result.scalars().all()
        
        # Группируем по названиям
        city_dict = {}
        for city in cities:
            name = city.name.lower()
            if name not in city_dict:
                city_dict[name] = []
            city_dict[name].append(city)
        
        # Удаляем дубликаты
        removed = 0
        for name, city_list in city_dict.items():
            if len(city_list) > 1:
                # Оставляем первый, удаляем остальные
                for city in city_list[1:]:
                    await session.delete(city)
                    removed += 1
                    logger.info(f"🗑️ Удален дубликат: {city.name} (ID: {city.id})")
        
        await session.commit()
        logger.info(f"✅ Удалено дубликатов: {removed}")
        
    finally:
        await session.close()

if __name__ == "__main__":
    asyncio.run(clean_duplicate_cities())