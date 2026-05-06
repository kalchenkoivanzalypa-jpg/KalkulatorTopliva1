#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite миграция: добавить расширенные поля в таблицу spimex_prices.

Запуск (на VPS):
  sudo -u fuel -H bash -lc 'cd /opt/fuel_bot && ./venv/bin/python3 scripts/migrate_sqlite_spimex_prices_add_fields.py --db /opt/fuel_bot/data/fuel_bot.db'

Идемпотентно: повторный запуск безопасен (поля не будут добавлены второй раз).
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


FIELDS: list[tuple[str, str]] = [
    ("price_market", "REAL"),
    ("price_avg", "REAL"),
    ("price_min", "REAL"),
    ("price_max", "REAL"),
    ("best_ask", "REAL"),
    ("best_bid", "REAL"),
    ("contracts", "INTEGER"),
    ("volume_rub", "REAL"),
    ("source_pdf", "TEXT"),
]


def _existing_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cols: set[str] = set()
    for row in conn.execute(f"PRAGMA table_info({table});"):
        # row = (cid, name, type, notnull, dflt_value, pk)
        cols.add(str(row[1]))
    return cols


def _ensure_indexes(conn: sqlite3.Connection) -> None:
    # Стараемся создать индекс, если его нет. IF NOT EXISTS есть у SQLite для index.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_spimex_code_date ON spimex_prices(exchange_product_id, date);"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_spimex_date ON spimex_prices(date);")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Путь к SQLite БД (fuel_bot.db)")
    args = ap.parse_args()
    db_path = Path(args.db).expanduser().resolve()
    if not db_path.is_file():
        raise SystemExit(f"Нет файла БД: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys=ON;")
        cols = _existing_columns(conn, "spimex_prices")
        added = 0
        for name, typ in FIELDS:
            if name in cols:
                continue
            try:
                conn.execute(f"ALTER TABLE spimex_prices ADD COLUMN {name} {typ};")
                added += 1
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
                cols = _existing_columns(conn, "spimex_prices")
        _ensure_indexes(conn)
        conn.commit()
        print(f"✅ migrate_spimex_prices_add_fields: added_columns={added}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

