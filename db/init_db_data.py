#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Инициализация справочников продуктов и базисов
Добавляет все продукты и базисы из instruments.py
"""

import asyncio
import logging
from db.database import Basis, Product, get_session
from sqlalchemy import select
from instruments import INSTRUMENTS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def init_products():
    """Заполняет таблицу products всеми уникальными продуктами из instruments.py"""
    
    # Собираем все уникальные продукты
    unique_products = {}
    for code, data in INSTRUMENTS.items():
        fuel_name = data["fuel"]
        if fuel_name not in unique_products:
            # Определяем описание на основе названия
            description = ""
            if "АИ" in fuel_name:
                description = "Бензин автомобильный"
            elif "ДТ" in fuel_name:
                description = "Дизельное топливо"
            elif "Мазут" in fuel_name:
                description = "Мазут топочный"
            elif "реактивных" in fuel_name:
                description = "Авиационный керосин"
            
            unique_products[fuel_name] = {
                "name": fuel_name,
                "description": description,
                "unit": "тонна",
                "is_active": True
            }
    
    session = await get_session()
    added = 0
    skipped = 0
    
    try:
        for fuel_name, prod_data in unique_products.items():
            # Проверяем, есть ли уже такой продукт
            existing = await session.execute(
                select(Product).where(Product.name == fuel_name)
            )
            if not existing.scalar_one_or_none():
                product = Product(**prod_data)
                session.add(product)
                added += 1
                logger.info(f"✅ Добавлен продукт: {fuel_name}")
            else:
                skipped += 1
                logger.debug(f"⏭️ Продукт уже существует: {fuel_name}")
        
        await session.commit()
        logger.info(f"📦 Продукты: добавлено {added}, пропущено {skipped}, всего {len(unique_products)}")
        
    finally:
        await session.close()
    
    return unique_products

async def init_basises():
    """Заполняет таблицу basises всеми уникальными базисами из instruments.py"""
    
    # Собираем все уникальные базисы
    unique_basises = {}
    for code, data in INSTRUMENTS.items():
        basis_name = data["basis"]
        if basis_name not in unique_basises:
            # Координаты нужно будет добавить позже через отдельный скрипт
            unique_basises[basis_name] = {
                "city": basis_name,
                "latitude": 0.0,  # Временно
                "longitude": 0.0,  # Временно
                "is_active": True
            }
    
    session = await get_session()
    added = 0
    skipped = 0
    
    try:
        for basis_name, basis_data in unique_basises.items():
            existing = await session.execute(
                select(Basis).where(Basis.city == basis_name)
            )
            if not existing.scalar_one_or_none():
                basis = Basis(**basis_data)
                session.add(basis)
                added += 1
                logger.info(f"✅ Добавлен базис: {basis_name}")
            else:
                skipped += 1
                logger.debug(f"⏭️ Базис уже существует: {basis_name}")
        
        await session.commit()
        logger.info(f"📍 Базисы: добавлено {added}, пропущено {skipped}, всего {len(unique_basises)}")
        
    finally:
        await session.close()
    
    return unique_basises

async def show_stats():
    """Показывает статистику по добавленным данным"""
    session = await get_session()
    
    try:
        # Считаем продукты
        products_result = await session.execute(select(Product))
        products = products_result.scalars().all()
        
        # Считаем базисы
        basises_result = await session.execute(select(Basis))
        basises = basises_result.scalars().all()
        
        logger.info("=" * 50)
        logger.info("📊 СТАТИСТИКА БАЗЫ ДАННЫХ")
        logger.info("=" * 50)
        logger.info(f"📦 Продуктов в БД: {len(products)}")
        logger.info(f"📍 Базисов в БД: {len(basises)}")
        
        # Показываем первые 10 продуктов
        if products:
            logger.info("\n📋 Первые 10 продуктов:")
            for p in sorted(products, key=lambda x: x.name)[:10]:
                logger.info(f"  • {p.name}")
        
        # Показываем первые 10 базисов
        if basises:
            logger.info("\n📋 Первые 10 базисов:")
            for b in sorted(basises, key=lambda x: x.city)[:10]:
                logger.info(f"  • {b.city}")
        
    finally:
        await session.close()

async def main():
    logger.info("=" * 60)
    logger.info("ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ")
    logger.info("=" * 60)
    
    logger.info(f"📊 Всего кодов в instruments.py: {len(INSTRUMENTS)}")
    
    print()
    logger.info("📦 ЗАГРУЗКА ПРОДУКТОВ...")
    products = await init_products()
    logger.info(f"   Найдено уникальных продуктов: {len(products)}")
    
    print()
    logger.info("📍 ЗАГРУЗКА БАЗИСОВ...")
    basises = await init_basises()
    logger.info(f"   Найдено уникальных базисов: {len(basises)}")
    
    print()
    await show_stats()
    
    logger.info("=" * 60)
    logger.info("✅ ИНИЦИАЛИЗАЦИЯ ЗАВЕРШЕНА")
    logger.info("=" * 60)

if __name__ == "__main__":
    asyncio.run(main())