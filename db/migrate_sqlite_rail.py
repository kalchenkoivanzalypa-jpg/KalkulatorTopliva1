# -*- coding: utf-8 -*-
"""
Добавляет в SQLite новые поля для Ж/Д у таблицы basis и создаёт rail_stations.

Путь к файлу БД берётся из DATABASE_URL (как у SQLAlchemy), чтобы не промахнуться
мимо fuel_bot.db в папке проекта.

Запуск из корня проекта:
  python3 -m db.migrate_sqlite_rail

Синхронная миграция только колонок basis (без asyncio):
  from db.migrate_sqlite_rail import apply_basis_sqlite_migrations
  apply_basis_sqlite_migrations()
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def get_sqlite_file_path() -> Optional[str]:
    """Абсолютный путь к файлу SQLite из DATABASE_URL."""
    try:
        from sqlalchemy.engine.url import make_url
    except ImportError:
        return None

    raw = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///fuel_bot.db")
    try:
        u = make_url(raw)
    except Exception:
        return None

    if "sqlite" not in u.drivername:
        return None
    if not u.database:
        return None

    path = u.database
    if not os.path.isabs(path):
        path = os.path.abspath(path)
    return path


def alter_basis_columns(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    for name, typ in (
        ("rail_station_name", "TEXT"),
        ("rail_esr", "TEXT"),
        ("rail_latitude", "REAL"),
        ("rail_longitude", "REAL"),
    ):
        try:
            cur.execute(f"ALTER TABLE basis ADD COLUMN {name} {typ}")
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "duplicate column" in msg or "already exists" in msg:
                continue
            raise
    conn.commit()


def apply_basis_sqlite_migrations() -> bool:
    """
    Добавляет недостающие колонки в basis. Возвращает True, если файл БД найден.
    """
    path = get_sqlite_file_path()
    if not path or not os.path.isfile(path):
        return False
    conn = sqlite3.connect(path)
    try:
        alter_basis_columns(conn)
    finally:
        conn.close()
    return True


async def create_all_tables() -> None:
    from db.database import Base, engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def main() -> None:
    path = get_sqlite_file_path()
    await create_all_tables()
    if path and os.path.isfile(path):
        conn = sqlite3.connect(path)
        try:
            alter_basis_columns(conn)
        finally:
            conn.close()
        print(f"✅ Миграция SQLite: {path}")
    else:
        print("ℹ️ Не SQLite или файл БД ещё не создан — выполнено только create_all()")


if __name__ == "__main__":
    asyncio.run(main())
