# check_all_coordinates.py
import asyncio
import logging
from db.database import Basis, get_session
from sqlalchemy import select

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def check_all_coordinates():
    """Проверка координат всех базисов"""
    session = await get_session()
    try:
        result = await session.execute(
            select(Basis).order_by(Basis.name)
        )
        basises = result.scalars().all()
        
        print("\n" + "="*80)
        print(f"{'БАЗИС':<40} {'ШИРОТА':<15} {'ДОЛГОТА':<15} {'ТРАНСПОРТ':<10}")
        print("="*80)
        
        for basis in basises:
            transport = "🚛" if basis.transport_type == 'auto' else "🚂"
            print(f"{basis.name[:40]:<40} {basis.latitude:<15.6f} {basis.longitude:<15.6f} {transport}")
        
        print("="*80)
        
        # Проверим конкретно Пурпе (в БД иногда встречается несколько строк)
        purpe_res = await session.execute(
            select(Basis).where(Basis.name.contains("Пурпе"))
        )
        purpe_list = purpe_res.scalars().all()

        if purpe_list:
            # Рассчитаем расстояние до Хабаровска
            from bot.handlers import calculate_distance
            habarovsk_lat, habarovsk_lon = 48.4802, 135.0719

            for purpe in purpe_list:
                print(f"\n📍 Базис: {purpe.name}")
                print(f"   Координаты: {purpe.latitude}, {purpe.longitude}")
                print(f"   Транспорт: {'Авто' if purpe.transport_type == 'auto' else 'Ж/Д'}")
                dist = calculate_distance(
                    purpe.latitude, purpe.longitude, habarovsk_lat, habarovsk_lon
                )
                print(f"   Расстояние до Хабаровска: {dist:.0f} км")
        
    finally:
        await session.close()

if __name__ == "__main__":
    asyncio.run(check_all_coordinates())