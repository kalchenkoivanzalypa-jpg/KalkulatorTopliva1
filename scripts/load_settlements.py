#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Загрузка населённых пунктов из data.csv в базу данных
Исправленная версия с правильной обработкой дубликатов
"""

import csv
import asyncio
import logging
from db.database import CityDestination, get_session
from sqlalchemy import select, func, and_

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CSV_FILE = "data.csv"
BATCH_SIZE = 500  # Увеличил размер пакета для скорости

async def load_settlements():
    """Загружает населённые пункты из CSV в таблицу CityDestination"""
    
    logger.info(f"📂 Загрузка населённых пунктов из {CSV_FILE}")
    
    session = await get_session()
    added = 0
    skipped = 0
    errors = 0
    batch = []  # Накопление для пакетной вставки
    
    try:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row_num, row in enumerate(reader, 1):
                try:
                    # Формируем название с типом поселения
                    settlement_type = row.get('type', '').strip()
                    name = row.get('settlement', '').strip()
                    region = row.get('region', '').strip()
                    
                    if not name or not region:
                        skipped += 1
                        continue
                    
                    if settlement_type:
                        full_name = f"{name} {settlement_type}"
                    else:
                        full_name = name
                    
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
                    
                    # Добавляем в пакет
                    city = CityDestination(
                        name=full_name,
                        region=region,
                        latitude=lat,
                        longitude=lon,
                        request_count=0
                    )
                    batch.append(city)
                    
                    # Если набрали достаточно, сохраняем
                    if len(batch) >= BATCH_SIZE:
                        saved = await save_batch(session, batch)
                        added += saved
                        batch = []
                        logger.info(f"💾 Сохранено {added} записей...")
                        
                except Exception as e:
                    logger.error(f"❌ Ошибка в строке {row_num}: {e}")
                    errors += 1
                    continue
            
            # Сохраняем остаток
            if batch:
                saved = await save_batch(session, batch)
                added += saved
            
            logger.info("=" * 60)
            logger.info(f"📊 ИТОГИ ЗАГРУЗКИ:")
            logger.info(f"   ✅ Добавлено: {added}")
            logger.info(f"   ⏭️  Пропущено: {skipped}")
            logger.info(f"   ❌ Ошибок: {errors}")
            logger.info("=" * 60)
            
    except FileNotFoundError:
        logger.error(f"❌ Файл {CSV_FILE} не найден!")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
    finally:
        await session.close()

async def save_batch(session, batch):
    """Сохраняет пакет записей, пропуская существующие"""
    saved = 0
    
    for city in batch:
        try:
            # Проверяем существование по имени И региону
            existing = await session.execute(
                select(CityDestination)
                .where(
                    and_(
                        CityDestination.name == city.name,
                        CityDestination.region == city.region
                    )
                )
            )
            
            if not existing.scalar_one_or_none():
                session.add(city)
                saved += 1
            else:
                logger.debug(f"⏭️ Уже есть: {city.name}, {city.region}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке {city.name}: {e}")
            await session.rollback()
            continue
    
    try:
        await session.commit()
    except Exception as e:
        logger.error(f"❌ Ошибка при сохранении пакета: {e}")
        await session.rollback()
        saved = 0
    
    return saved

async def check_stats():
    """Проверяет статистику по загруженным данным"""
    
    session = await get_session()
    try:
        # Общее количество
        total = await session.execute(select(func.count()).select_from(CityDestination))
        total = total.scalar()
        
        # Количество уникальных названий
        unique_names = await session.execute(
            select(func.count()).select_from(
                select(CityDestination.name).distinct().subquery()
            )
        )
        unique_names = unique_names.scalar()
        
        # Топ-10 регионов по количеству населённых пунктов
        top_regions = await session.execute(
            select(CityDestination.region, func.count().label('cnt'))
            .group_by(CityDestination.region)
            .order_by(func.count().desc())
            .limit(10)
        )
        top_regions = top_regions.all()
        
        logger.info("=" * 60)
        logger.info("📊 СТАТИСТИКА БАЗЫ ДАННЫХ:")
        logger.info(f"   📍 Всего населённых пунктов: {total}")
        logger.info(f"   🏷️  Уникальных названий: {unique_names}")
        logger.info("\n   📌 Топ-10 регионов:")
        for region, count in top_regions:
            logger.info(f"      • {region}: {count}")
        logger.info("=" * 60)
        
    finally:
        await session.close()

async def main():
    logger.info("🚀 НАЧАЛО ЗАГРУЗКИ НАСЕЛЁННЫХ ПУНКТОВ")
    
    # Загружаем данные
    await load_settlements()
    
    # Показываем статистику
    await check_stats()
    
    logger.info("✅ ЗАГРУЗКА ЗАВЕРШЕНА")

if __name__ == "__main__":
    asyncio.run(main())