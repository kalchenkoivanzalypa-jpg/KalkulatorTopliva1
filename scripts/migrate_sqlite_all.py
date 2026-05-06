#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Один прогон всех идемпотентных SQLite-миграций (после деплоя / при рассинхроне схемы).

  sudo -u fuel -H bash -lc 'cd /opt/fuel_bot && ./venv/bin/python3 scripts/migrate_sqlite_all.py --db /opt/fuel_bot/fuel_bot.db'

При uvicorn --workers N несколько процессов вызывают init_db одновременно — без блокировки
второй воркер ловит duplicate column. fcntl на файле БД сериализует миграции.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

try:
    import fcntl
except ImportError:
    fcntl = None  # type: ignore[assignment]


@contextmanager
def _exclusive_db_file_lock(db_path: Path):
    if fcntl is None:
        yield
        return
    f = open(db_path, "r+b")
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            # correct constant name in python's fcntl
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        f.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Путь к fuel_bot.db")
    args = ap.parse_args()
    db = Path(args.db).expanduser().resolve()
    if not db.is_file():
        print(f"FAIL: нет файла БД: {db}", file=sys.stderr)
        return 2

    py = sys.executable
    steps: list[list[str]] = [
        [py, str(ROOT / "scripts/migrate_sqlite_spimex_prices_add_fields.py"), "--db", str(db)],
        [py, str(ROOT / "scripts/migrate_sqlite_add_max_user_id.py"), str(db)],
        [py, str(ROOT / "scripts/migrate_sqlite_price_alerts_add_basis_id.py"), "--db", str(db)],
        [py, str(ROOT / "scripts/migrate_sqlite_basis_digest_subscriptions.py"), "--db", str(db)],
        [py, str(ROOT / "scripts/migrate_sqlite_user_destinations.py"), "--db", str(db)],
        [py, str(ROOT / "scripts/migrate_sqlite_anomaly_alerts_direction.py"), "--db", str(db)],
        [py, str(ROOT / "scripts/migrate_sqlite_table_digest_subscriptions.py"), "--db", str(db)],
    ]
    with _exclusive_db_file_lock(db):
        for cmd in steps:
            print("+", " ".join(cmd), flush=True)
            subprocess.check_call(cmd, cwd=str(ROOT))
    print("OK migrate_sqlite_all", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
