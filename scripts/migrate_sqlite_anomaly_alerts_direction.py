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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to sqlite db file")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

    con = sqlite3.connect(args.db)
    try:
        cur = con.cursor()
        added = 0

        if not _col_exists(cur, "anomaly_alerts", "direction"):
            logger.info("Adding column anomaly_alerts.direction")
            try:
                cur.execute("ALTER TABLE anomaly_alerts ADD COLUMN direction TEXT NOT NULL DEFAULT 'any'")
                added += 1
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise

        # Backfill any NULL/empty
        cur.execute("UPDATE anomaly_alerts SET direction='any' WHERE direction IS NULL OR trim(direction)=''")

        con.commit()
        logger.info("Done. added_columns=%s", added)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

