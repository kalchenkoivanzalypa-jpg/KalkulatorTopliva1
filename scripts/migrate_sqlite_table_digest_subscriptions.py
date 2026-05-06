#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Таблица table_digest_subscriptions (ежедневная пользовательская таблица с доставкой)."""
from __future__ import annotations

import argparse
import logging
import sqlite3

logger = logging.getLogger(__name__)


def _table_exists(cur: sqlite3.Cursor, table: str) -> bool:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

    con = sqlite3.connect(args.db)
    try:
        cur = con.cursor()
        if not _table_exists(cur, "table_digest_subscriptions"):
            logger.info("Creating table table_digest_subscriptions")
            cur.execute(
                """
                CREATE TABLE table_digest_subscriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    product_ids_json TEXT NOT NULL,
                    basis_ids_json TEXT NOT NULL,
                    destination_ids_json TEXT NOT NULL,
                    send_hour_msk INTEGER NOT NULL DEFAULT 14,
                    send_minute_msk INTEGER NOT NULL DEFAULT 15,
                    notify_email INTEGER NOT NULL DEFAULT 1,
                    notify_max INTEGER NOT NULL DEFAULT 1,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    last_sent_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_table_digest_user_active ON table_digest_subscriptions(user_id, is_active)"
            )
            con.commit()
        logger.info("OK table_digest_subscriptions")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
