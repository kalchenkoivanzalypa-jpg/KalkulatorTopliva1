#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import logging
import sqlite3

logger = logging.getLogger(__name__)


def _col_exists(cur: sqlite3.Cursor, table: str, col: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1] == col for row in cur.fetchall())


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
        added = 0

        if not _col_exists(cur, "price_alerts", "basis_id"):
            logger.info("Adding column price_alerts.basis_id")
            try:
                cur.execute("ALTER TABLE price_alerts ADD COLUMN basis_id INTEGER")
                added += 1
            except sqlite3.OperationalError as e:
                # Параллельные воркеры uvicorn: колонка уже добавлена другим процессом
                if "duplicate column" not in str(e).lower():
                    raise

        if not _idx_exists(cur, "idx_price_alerts_active_basis"):
            logger.info("Creating index idx_price_alerts_active_basis")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_price_alerts_active_basis ON price_alerts(is_active, basis_id)")

        con.commit()
        logger.info("Done. added_columns=%s", added)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

