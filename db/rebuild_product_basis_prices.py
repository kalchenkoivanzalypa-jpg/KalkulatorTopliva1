# -*- coding: utf-8 -*-
"""
Пересоздаёт таблицу product_basis_prices под схему с instrument_code.

ВНИМАНИЕ: сбрасывает все цены; в user_requests обнуляет price_id.

  python3 -m db.rebuild_product_basis_prices
"""
from __future__ import annotations

import asyncio

import os

from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()


async def main() -> None:
    from db.database import ProductBasisPrice, engine

    if "sqlite" not in (os.getenv("DATABASE_URL") or "").lower():
        raise SystemExit("Скрипт рассчитан на SQLite; для другой СУБД сделайте миграцию вручную.")

    async with engine.begin() as conn:

        await conn.execute(text("UPDATE user_requests SET price_id = NULL"))
        await conn.execute(text("DROP TABLE IF EXISTS product_basis_prices"))

        def create_pbp(sync_conn):
            ProductBasisPrice.__table__.create(sync_conn, checkfirst=True)

        await conn.run_sync(create_pbp)

    print("✅ Таблица product_basis_prices пересоздана. Загрузите баба.xlsx, Базисы поставок.xlsx и PDF.")


if __name__ == "__main__":
    asyncio.run(main())
