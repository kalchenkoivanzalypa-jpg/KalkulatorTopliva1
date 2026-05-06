#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Создание таблицы basis_digest_subscriptions и частичных UNIQUE-индексов (SQLite)."""
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


def _col_exists(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    cur.execute(f"PRAGMA table_info({table});")
    rows = cur.fetchall() or []
    for r in rows:
        try:
            name = str(r[1])
        except Exception:
            continue
        if name == col:
            return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to sqlite db file")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

    con = sqlite3.connect(args.db)
    try:
        cur = con.cursor()
        if not _table_exists(cur, "basis_digest_subscriptions"):
            logger.info("Creating table basis_digest_subscriptions")
            cur.execute(
                """
                CREATE TABLE basis_digest_subscriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    basis_id INTEGER NOT NULL REFERENCES basis(id),
                    -- prices_only | with_delivery
                    delivery_mode TEXT NOT NULL DEFAULT 'prices_only',
                    -- destination for with_delivery (optional for prices_only)
                    destination_id INTEGER REFERENCES city_destinations(id),
                    destination_name TEXT,
                    destination_key TEXT,
                    all_products INTEGER NOT NULL DEFAULT 0,
                    product_id INTEGER REFERENCES products(id),
                    is_active INTEGER NOT NULL DEFAULT 1,
                    last_sent_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_basis_digest_user ON basis_digest_subscriptions(user_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_basis_digest_active ON basis_digest_subscriptions(is_active)"
            )
        else:
            # additive columns for old DBs
            if not _col_exists(cur, "basis_digest_subscriptions", "delivery_mode"):
                logger.info("Adding column basis_digest_subscriptions.delivery_mode")
                cur.execute(
                    "ALTER TABLE basis_digest_subscriptions ADD COLUMN delivery_mode TEXT NOT NULL DEFAULT 'prices_only'"
                )
            if not _col_exists(cur, "basis_digest_subscriptions", "destination_id"):
                logger.info("Adding column basis_digest_subscriptions.destination_id")
                cur.execute(
                    "ALTER TABLE basis_digest_subscriptions ADD COLUMN destination_id INTEGER REFERENCES city_destinations(id)"
                )
            if not _col_exists(cur, "basis_digest_subscriptions", "destination_name"):
                logger.info("Adding column basis_digest_subscriptions.destination_name")
                cur.execute("ALTER TABLE basis_digest_subscriptions ADD COLUMN destination_name TEXT")
            if not _col_exists(cur, "basis_digest_subscriptions", "destination_key"):
                logger.info("Adding column basis_digest_subscriptions.destination_key")
                cur.execute("ALTER TABLE basis_digest_subscriptions ADD COLUMN destination_key TEXT")

        # Unique indexes: allow same basis/product if destination differs for with_delivery.
        if not _idx_exists(cur, "uq_basis_digest_prices_only_all"):
            logger.info("Creating partial unique index uq_basis_digest_prices_only_all")
            cur.execute(
                """
                CREATE UNIQUE INDEX uq_basis_digest_prices_only_all
                ON basis_digest_subscriptions(user_id, basis_id)
                WHERE all_products = 1 AND delivery_mode = 'prices_only' AND destination_id IS NULL
                """
            )
        if not _idx_exists(cur, "uq_basis_digest_prices_only_product"):
            logger.info("Creating partial unique index uq_basis_digest_prices_only_product")
            cur.execute(
                """
                CREATE UNIQUE INDEX uq_basis_digest_prices_only_product
                ON basis_digest_subscriptions(user_id, basis_id, product_id)
                WHERE all_products = 0 AND product_id IS NOT NULL
                  AND delivery_mode = 'prices_only' AND destination_id IS NULL
                """
            )
        if not _idx_exists(cur, "uq_basis_digest_delivery_all"):
            logger.info("Creating partial unique index uq_basis_digest_delivery_all")
            cur.execute(
                """
                CREATE UNIQUE INDEX uq_basis_digest_delivery_all
                ON basis_digest_subscriptions(user_id, basis_id, destination_id)
                WHERE all_products = 1 AND delivery_mode = 'with_delivery' AND destination_id IS NOT NULL
                """
            )
        if not _idx_exists(cur, "uq_basis_digest_delivery_product"):
            logger.info("Creating partial unique index uq_basis_digest_delivery_product")
            cur.execute(
                """
                CREATE UNIQUE INDEX uq_basis_digest_delivery_product
                ON basis_digest_subscriptions(user_id, basis_id, product_id, destination_id)
                WHERE all_products = 0 AND product_id IS NOT NULL
                  AND delivery_mode = 'with_delivery' AND destination_id IS NOT NULL
                """
            )

        con.commit()
        logger.info("Done.")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
