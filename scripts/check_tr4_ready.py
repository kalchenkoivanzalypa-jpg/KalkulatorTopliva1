#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(description="Проверка, что локальные книги TR4 на месте")
    ap.add_argument("--root", default=".", help="Корень проекта (по умолчанию текущая папка)")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    data = root / "railway" / "data"
    k1 = data / "kniga1"
    k2 = data / "kniga2"
    k3 = data / "kniga3"

    def _count_csv(p: Path) -> int:
        if not p.is_dir():
            return 0
        return len(list(p.glob("*.csv")))

    print("Project:", root)
    print("Data:", data, "exists" if data.is_dir() else "MISSING")
    print("kniga1:", k1, "csv:", _count_csv(k1))
    print("kniga2:", k2, "csv:", _count_csv(k2))
    print("kniga3:", k3, "csv:", _count_csv(k3))

    if not k1.is_dir() or _count_csv(k1) == 0:
        print("❌ kniga1 отсутствует/пустая — TR4 будет неполным.")
    if not k2.is_dir() or _count_csv(k2) == 0:
        print("❌ kniga2 отсутствует/пустая — ESR→станция не будет работать.")
    if not k3.is_dir() or _count_csv(k3) == 0:
        print("⚠️ kniga3 отсутствует/пустая — будет использоваться только Книга 1 (если получится).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

