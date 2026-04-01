#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Добавление городов Дальнего Востока в базу данных
Исправленная версия с обработкой дубликатов
"""

import asyncio
import logging
from db.database import CityDestination, get_session
from sqlalchemy import select, and_

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Города Дальнего Востока с координатами
FAR_EAST_CITIES = [
    # Сахалинская область
    {"name": "Южно-Сахалинск", "region": "Сахалинская область", "lat": 46.9592, "lon": 142.7381},
    {"name": "Корсаков", "region": "Сахалинская область", "lat": 46.6324, "lon": 142.7885},
    {"name": "Холмск", "region": "Сахалинская область", "lat": 47.0478, "lon": 142.0569},
    {"name": "Оха", "region": "Сахалинская область", "lat": 53.5738, "lon": 142.9478},
    {"name": "Поронайск", "region": "Сахалинская область", "lat": 49.2218, "lon": 143.1001},
    {"name": "Долинск", "region": "Сахалинская область", "lat": 47.3259, "lon": 142.7951},
    {"name": "Невельск", "region": "Сахалинская область", "lat": 46.6527, "lon": 141.8615},
    {"name": "Анива", "region": "Сахалинская область", "lat": 46.7156, "lon": 142.5277},
    {"name": "Углегорск", "region": "Сахалинская область", "lat": 49.0815, "lon": 142.0323},
    {"name": "Александровск-Сахалинский", "region": "Сахалинская область", "lat": 50.8999, "lon": 142.1575},
    
    # Приморский край
    {"name": "Владивосток", "region": "Приморский край", "lat": 43.1155, "lon": 131.8855},
    {"name": "Находка", "region": "Приморский край", "lat": 42.8243, "lon": 132.8926},
    {"name": "Уссурийск", "region": "Приморский край", "lat": 43.7972, "lon": 131.9527},
    {"name": "Артем", "region": "Приморский край", "lat": 43.3501, "lon": 132.1881},
    {"name": "Арсеньев", "region": "Приморский край", "lat": 44.1625, "lon": 133.2736},
    {"name": "Спасск-Дальний", "region": "Приморский край", "lat": 44.5971, "lon": 132.8197},
    {"name": "Большой Камень", "region": "Приморский край", "lat": 43.1186, "lon": 132.3519},
    {"name": "Партизанск", "region": "Приморский край", "lat": 43.1284, "lon": 133.1235},
    {"name": "Лесозаводск", "region": "Приморский край", "lat": 45.4786, "lon": 133.4189},
    {"name": "Дальнереченск", "region": "Приморский край", "lat": 45.9308, "lon": 133.7381},
    {"name": "Фокино", "region": "Приморский край", "lat": 42.9734, "lon": 132.8069},
    
    # Хабаровский край
    {"name": "Хабаровск", "region": "Хабаровский край", "lat": 48.4802, "lon": 135.0719},
    {"name": "Комсомольск-на-Амуре", "region": "Хабаровский край", "lat": 50.5501, "lon": 137.0083},
    {"name": "Амурск", "region": "Хабаровский край", "lat": 50.2168, "lon": 136.8891},
    {"name": "Советская Гавань", "region": "Хабаровский край", "lat": 48.9736, "lon": 140.2953},
    {"name": "Николаевск-на-Амуре", "region": "Хабаровский край", "lat": 53.1466, "lon": 140.7103},
    {"name": "Бикин", "region": "Хабаровский край", "lat": 46.8166, "lon": 134.2559},
    {"name": "Вяземский", "region": "Хабаровский край", "lat": 47.5375, "lon": 134.7566},
    
    # Камчатский край
    {"name": "Петропавловск-Камчатский", "region": "Камчатский край", "lat": 53.0371, "lon": 158.6489},
    {"name": "Елизово", "region": "Камчатский край", "lat": 53.1829, "lon": 158.3884},
    {"name": "Вилючинск", "region": "Камчатский край", "lat": 52.9315, "lon": 158.4049},
    
    # Магаданская область
    {"name": "Магадан", "region": "Магаданская область", "lat": 59.5612, "lon": 150.8301},
    {"name": "Сусуман", "region": "Магаданская область", "lat": 62.7845, "lon": 148.1587},
    
    # Чукотка
    {"name": "Анадырь", "region": "Чукотский АО", "lat": 64.7349, "lon": 177.5143},
    {"name": "Певек", "region": "Чукотский АО", "lat": 69.7015, "lon": 170.2994},
    {"name": "Билибино", "region": "Чукотский АО", "lat": 68.0546, "lon": 166.4576},
    
    # Якутия
    {"name": "Якутск", "region": "Республика Саха (Якутия)", "lat": 62.0278, "lon": 129.7325},
    {"name": "Нерюнгри", "region": "Республика Саха (Якутия)", "lat": 56.6601, "lon": 124.7202},
    {"name": "Мирный", "region": "Республика Саха (Якутия)", "lat": 62.5432, "lon": 113.9588},
    {"name": "Ленск", "region": "Республика Саха (Якутия)", "lat": 60.7278, "lon": 114.9312},
    {"name": "Алдан", "region": "Республика Саха (Якутия)", "lat": 58.6031, "lon": 125.3955},
    {"name": "Вилюйск", "region": "Республика Саха (Якутия)", "lat": 63.7555, "lon": 121.6247},
    
    # Амурская область
    {"name": "Благовещенск", "region": "Амурская область", "lat": 50.2907, "lon": 127.5272},
    {"name": "Белогорск", "region": "Амурская область", "lat": 50.9158, "lon": 128.4753},
    {"name": "Свободный", "region": "Амурская область", "lat": 51.3807, "lon": 128.1285},
    {"name": "Тында", "region": "Амурская область", "lat": 55.1559, "lon": 124.7245},
    {"name": "Зея", "region": "Амурская область", "lat": 53.7357, "lon": 127.2626},
    {"name": "Шимановск", "region": "Амурская область", "lat": 52.0053, "lon": 127.6788},
    {"name": "Райчихинск", "region": "Амурская область", "lat": 49.7945, "lon": 129.4113},
    
    # Еврейская АО
    {"name": "Биробиджан", "region": "Еврейская АО", "lat": 48.7945, "lon": 132.9219},
    
    # Забайкальский край
    {"name": "Чита", "region": "Забайкальский край", "lat": 52.0346, "lon": 113.4995},
    {"name": "Краснокаменск", "region": "Забайкальский край", "lat": 50.0926, "lon": 118.0323},
    {"name": "Борзя", "region": "Забайкальский край", "lat": 50.3879, "lon": 116.5235},
    {"name": "Петровск-Забайкальский", "region": "Забайкальский край", "lat": 51.2741, "lon": 108.8461},
    
    # Бурятия
    {"name": "Улан-Удэ", "region": "Республика Бурятия", "lat": 51.8335, "lon": 107.5841},
    {"name": "Северобайкальск", "region": "Республика Бурятия", "lat": 55.6358, "lon": 109.3192},
    {"name": "Гусиноозерск", "region": "Республика Бурятия", "lat": 51.2881, "lon": 106.5239},
    
    # Иркутская область (восточная часть)
    {"name": "Иркутск", "region": "Иркутская область", "lat": 52.2864, "lon": 104.2806},
    {"name": "Братск", "region": "Иркутская область", "lat": 56.1513, "lon": 101.6343},
    {"name": "Ангарск", "region": "Иркутская область", "lat": 52.5443, "lon": 103.8875},
    {"name": "Усть-Илимск", "region": "Иркутская область", "lat": 58.0005, "lon": 102.6607},
    {"name": "Усолье-Сибирское", "region": "Иркутская область", "lat": 52.7515, "lon": 103.6495},
    {"name": "Черемхово", "region": "Иркутская область", "lat": 53.1612, "lon": 103.0665},
    {"name": "Тулун", "region": "Иркутская область", "lat": 54.5635, "lon": 100.5815},
    {"name": "Нижнеудинск", "region": "Иркутская область", "lat": 54.8974, "lon": 99.0282},
    {"name": "Тайшет", "region": "Иркутская область", "lat": 55.9342, "lon": 98.0042},
]

async def add_far_east_cities():
    """Добавляет города Дальнего Востока в базу данных"""
    
    session = await get_session()
    added = 0
    skipped = 0
    errors = 0
    
    try:
        for city_data in FAR_EAST_CITIES:
            try:
                # Проверяем, есть ли уже такой город (по точному названию и региону)
                result = await session.execute(
                    select(CityDestination).where(
                        and_(
                            CityDestination.name.ilike(f"%{city_data['name']}%"),
                            CityDestination.region == city_data['region']
                        )
                    )
                )
                existing = result.scalar_one_or_none()
                
                if not existing:
                    # Создаем новый город
                    new_city = CityDestination(
                        name=f"{city_data['name']} г",
                        region=city_data['region'],
                        latitude=city_data['lat'],
                        longitude=city_data['lon'],
                        request_count=0
                    )
                    session.add(new_city)
                    added += 1
                    logger.info(f"✅ Добавлен: {city_data['name']} ({city_data['region']})")
                    
                    # Сохраняем каждые 10 записей
                    if added % 10 == 0:
                        await session.commit()
                        logger.info(f"💾 Сохранено {added} записей...")
                        
                else:
                    skipped += 1
                    logger.info(f"⏭️ Уже есть: {city_data['name']} ({city_data['region']})")
                    
            except Exception as e:
                logger.error(f"❌ Ошибка при добавлении {city_data['name']}: {e}")
                errors += 1
                await session.rollback()
                continue
        
        # Финальное сохранение
        await session.commit()
        
        print("\n" + "=" * 60)
        print(f"📊 ИТОГИ:")
        print(f"   ✅ Добавлено: {added}")
        print(f"   ⏭️  Пропущено: {skipped}")
        print(f"   ❌ Ошибок: {errors}")
        print("=" * 60)
        
    finally:
        await session.close()

async def check_sakhalin():
    """Проверяет наличие Сахалинских городов"""
    
    session = await get_session()
    try:
        result = await session.execute(
            select(CityDestination).where(
                CityDestination.region.like("%Сахалин%")
            )
        )
        cities = result.scalars().all()
        
        if cities:
            print(f"\n📍 САХАЛИНСКИЕ ГОРОДА В БАЗЕ ({len(cities)}):")
            for city in cities:
                print(f"  • {city.name}")
        else:
            print("\n❌ Сахалинских городов пока нет")
            
    finally:
        await session.close()

async def check_yuzhno():
    """Проверяет Южно-Сахалинск"""
    
    session = await get_session()
    try:
        result = await session.execute(
            select(CityDestination).where(
                CityDestination.name.ilike("%Южно-Сахалинск%")
            )
        )
        cities = result.scalars().all()
        
        if cities:
            print(f"\n📍 ЮЖНО-САХАЛИНСК:")
            for city in cities:
                print(f"  • {city.name}, {city.region}")
        else:
            print("\n❌ Южно-Сахалинск не найден")
            
    finally:
        await session.close()

async def main():
    print("🚀 ДОБАВЛЕНИЕ ГОРОДОВ ДАЛЬНЕГО ВОСТОКА")
    print("=" * 60)
    
    await add_far_east_cities()
    await check_sakhalin()
    await check_yuzhno()
    
    print("\n✅ ЗАВЕРШЕНО")

if __name__ == "__main__":
    asyncio.run(main())