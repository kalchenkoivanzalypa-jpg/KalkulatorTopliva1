#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Создание таблицы user_destinations (SQLite): сохранённые направления пользователя."""
from __future__ import annotations

import argparse
import logging
import sqlite3

logger = logging.getLogger(__name__)


def _table_exists(cur: sqlite3.Cursor, table: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def _idx_exists(cur: sqlite3.Cursor, idx: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?", (idx,))
    return cur.fetchone() is not None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to sqlite db file")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

    con = sqlite3.connect(args.db)
    try:
        cur = con.cursor()

        if not _table_exists(cur, "user_destinations"):
            logger.info("Creating table user_destinations")
            cur.execute(
                """
                CREATE TABLE user_destinations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    city_destination_id INTEGER NOT NULL REFERENCES city_destinations(id),
                    label TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_used TIMESTAMP
                )
                """
            )
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_user_destinations_user_city "
                "ON user_destinations(user_id, city_destination_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_user_destinations_active "
                "ON user_destinations(user_id, is_active)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_user_destinations_city "
                "ON user_destinations(city_destination_id)"
            )
        else:
            # old DB: ensure indexes exist
            if not _idx_exists(cur, "uq_user_destinations_user_city"):
                logger.info("Creating unique index uq_user_destinations_user_city")
                cur.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_user_destinations_user_city "
                    "ON user_destinations(user_id, city_destination_id)"
                )
            if not _idx_exists(cur, "idx_user_destinations_active"):
                logger.info("Creating index idx_user_destinations_active")
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_user_destinations_active "
                    "ON user_destinations(user_id, is_active)"
                )
            if not _idx_exists(cur, "idx_user_destinations_city"):
                logger.info("Creating index idx_user_destinations_city")
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_user_destinations_city "
                    "ON user_destinations(city_destination_id)"
                )

        con.commit()
        logger.info("Done.")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

