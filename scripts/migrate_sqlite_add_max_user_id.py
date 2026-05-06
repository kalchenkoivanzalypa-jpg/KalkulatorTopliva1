#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite migration: add users.max_user_id for MAX messenger integration.

Usage:
  python scripts/migrate_sqlite_add_max_user_id.py /path/to/fuel_bot.db
"""

from __future__ import annotations

import sqlite3
import sys


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: python scripts/migrate_sqlite_add_max_user_id.py /path/to/fuel_bot.db")
        return 2

    db_path = sys.argv[1]
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cols = [r[1] for r in cur.execute("PRAGMA table_info(users)").fetchall()]
        if "max_user_id" not in cols:
            try:
                cur.execute("ALTER TABLE users ADD COLUMN max_user_id BIGINT")
                conn.commit()
                print("✅ Added users.max_user_id")
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    raise
                print("ℹ️ users.max_user_id already exists (parallel migrate)")
        else:
            print("ℹ️ users.max_user_id already exists")

        # Optional index for lookup performance
        cur.execute("CREATE INDEX IF NOT EXISTS idx_users_max_user_id ON users(max_user_id)")
        conn.commit()
        print("✅ Ensured index idx_users_max_user_id")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

