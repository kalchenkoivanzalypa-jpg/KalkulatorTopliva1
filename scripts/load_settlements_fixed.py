#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Надёжная загрузка населённых пунктов с обработкой дубликатов
"""

import csv
import asyncio
import logging
from db.database import CityDestination, get_session
from sqlalchemy import select, func, and_
from sqlalchemy.exc import IntegrityError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CSV_FILE = "data.csv"
BATCH_SIZE = 1000  # Увеличим для скорости

async def load_settlements_safe():
    """Безопасная загрузка с игнорированием дубликатов"""
    
    logger.info(f"📂 Загрузка населённых пунктов из {CSV_FILE}")
    
    session = await get_session()
    added = 0
    skipped = 0
    errors = 0
    batch = []
    
    try:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row_num, row in enumerate(reader, 1):
                try:
                    # Формируем название
                    settlement_type = row.get('type', '').strip()
                    name = row.get('settlement', '').strip()
                    region = row.get('region', '').strip()
                    
                    if not name or not region:
                        skipped += 1
                        continue
                    
                    full_name = f"{name} {settlement_type}" if settlement_type else name
                    
                    # Получаем координаты
                    try:
                        lat = float(row.get('latitude_dd', 0))
                        lon = float(row.get('longitude_dd', 0))
                    except (ValueError, TypeError):
                        errors += 1
                        continue
                    
                    if lat == 0 or lon == 0:
                        errors += 1
                        continue
                    
                    # Создаём объект
                    city = CityDestination(
                        name=full_name,
                        region=region,
                        latitude=lat,
                        longitude=lon,
                        request_count=0
                    )
                    batch.append(city)
                    
                    # Сохраняем пакет
                    if len(batch) >= BATCH_SIZE:
                        saved = await save_batch_safe(session, batch)
                        added += saved
                        logger.info(f"💾 Сохранено {added} записей...")
                        batch = []
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка в строке {row_num}: {e}")
                    errors += 1
                    continue
            
            # Сохраняем остаток
            if batch:
                saved = await save_batch_safe(session, batch)
                added += saved
            
            logger.info("=" * 60)
            logger.info(f"📊 ИТОГИ ЗАГРУЗКИ:")
            logger.info(f"   ✅ Добавлено: {added}")
            logger.info(f"   ⏭️  Пропущено: {skipped}")
            logger.info(f"   ❌ Ошибок: {errors}")
            logger.info("=" * 60)
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
    finally:
        await session.close()

async def save_batch_safe(session, batch):
    """Безопасное сохранение пакета с игнорированием IntegrityError"""
    saved = 0
    
    for city in batch:
        try:
            session.add(city)
            await session.flush()  # Пытаемся сохранить
            saved += 1
        except IntegrityError:
            # Дубликат - откатываем только эту запись
            await session.rollback()
            logger.debug(f"⏭️ Пропущен дубликат: {city.name}, {city.region}")
        except Exception as e:
            logger.error(f"❌ Ошибка при сохранении {city.name}: {e}")
            await session.rollback()
    
    try:
        await session.commit()
    except Exception as e:
        logger.error(f"❌ Ошибка при фиксации пакета: {e}")
        await session.rollback()
        saved = 0
    
    return saved

async def check_stats():
    """Проверка статистики"""
    session = await get_session()
    try:
        # Общее количество
        total = await session.execute(select(func.count()).select_from(CityDestination))
        total = total.scalar()
        
        # Количество уникальных названий
        unique = await session.execute(
            select(func.count()).select_from(
                select(CityDestination.name).distinct().subquery()
            )
        )
        unique = unique.scalar()
        
        logger.info(f"\n📊 Статистика: всего {total} записей, {unique} уникальных названий")
        
    finally:
        await session.close()

async def main():
    logger.info("🚀 Запуск загрузки")
    await load_settlements_safe()
    await check_stats()
    logger.info("✅ Загрузка завершена")

if __name__ == "__main__":
    asyncio.run(main())