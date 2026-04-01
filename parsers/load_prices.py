#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

"""
Загружает найденные цены из PDF в базу данных
"""

import asyncio
import logging
from db.database import Basis, Product, ProductBasisPrice, get_session
from sqlalchemy import select
from parsers.code_parser import parse_bulletin_by_codes, print_results
import glob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def load_prices_to_db(pdf_path=None):
    if not pdf_path:
        pdf_files = glob.glob("data/bulletins/*.pdf")
        if not pdf_files:
            logger.error("❌ Нет PDF файлов")
            return
        pdf_path = max(pdf_files, key=os.path.getctime)
    
    logger.info(f"📄 Обработка: {pdf_path}")
    results = parse_bulletin_by_codes(pdf_path)
    print_results(results)
    
    session = await get_session()
    updated = 0
    errors = 0
    
    try:
        for code, data in results.items():
            if data["market_price"] == 0:
                continue
            
            product = await session.execute(
                select(Product).where(Product.name == data["fuel"])
            )
            product = product.scalar_one_or_none()
            
            basis = await session.execute(
                select(Basis).where(Basis.city == data["basis"])
            )
            basis = basis.scalar_one_or_none()
            
            if not product or not basis:
                errors += 1
                continue
            
            existing = await session.execute(
                select(ProductBasisPrice)
                .where(ProductBasisPrice.product_id == product.id)
                .where(ProductBasisPrice.basis_id == basis.id)
            )
            existing = existing.scalar_one_or_none()
            
            if existing:
                existing.current_price = data["market_price"]
                logger.info(f"🔄 {data['fuel']} @ {data['basis']} = {data['market_price']}")
            else:
                new_price = ProductBasisPrice(
                    product_id=product.id,
                    basis_id=basis.id,
                    current_price=data["market_price"]
                )
                session.add(new_price)
                logger.info(f"➕ {data['fuel']} @ {data['basis']} = {data['market_price']}")
            
            updated += 1
        
        await session.commit()
        logger.info(f"✅ Загружено: {updated}, Ошибок: {errors}")
    finally:
        await session.close()

if __name__ == "__main__":
    asyncio.run(load_prices_to_db())