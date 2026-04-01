#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Проверка рыночных цен в БД по базисам: Ангарск-группа станций, СН КНПЗ, Дзёмги.

Пример:
  python3 check_basis_prices.py
  python3 check_basis_prices.py --csv report.csv
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import re
from typing import Dict, List, Optional, Sequence, Tuple

from sqlalchemy import select

from db.database import AsyncSessionLocal, Basis, Product, ProductBasisPrice


def _norm(s: str) -> str:
    return (s or "").lower().replace("ё", "е").strip()


# Порядок в отчёте; классификация — первая подходящая ветка (см. classify_basis)
GROUP_ORDER: Tuple[str, ...] = (
    "Ангарск-группа станций",
    "СН КНПЗ",
    "Дзёмги",
)


def classify_basis(basis_name: str) -> Optional[str]:
    """
    Сопоставление поля basis.name с группой отчёта.
    «СН КНПЗ / ст. Дземги» попадает в СН КНПЗ; «ст. Дземги» без КНПЗ — в Дзёмги.
    """
    n = _norm(basis_name)
    if "ангарск" in n and "группа" in n and ("стан" in n or "станц" in n):
        return "Ангарск-группа станций"
    if "кнпз" in n:
        return "СН КНПЗ"
    if "дзем" in n or re.search(r"дз(е|ё)мги", n) or re.search(r"ст\.\s*дз(е|ё)?м", n):
        return "Дзёмги"
    return None


async def fetch_rows() -> List[Tuple[str, str, str, str, float, Optional[str]]]:
    """
    (group_key, basis_name, product_name, instrument_code, price, last_updated iso)
    """
    out: List[Tuple[str, str, str, str, float, Optional[str]]] = []
    async with AsyncSessionLocal() as session:
        q = await session.execute(
            select(ProductBasisPrice, Product, Basis)
            .join(Product, ProductBasisPrice.product_id == Product.id)
            .join(Basis, ProductBasisPrice.basis_id == Basis.id)
            .where(
                ProductBasisPrice.is_active.is_(True),
                Basis.is_active.is_(True),
                Product.is_active.is_(True),
            )
            .order_by(Basis.name, Product.name)
        )
        for pbp, prod, basis in q.all():
            g = classify_basis(basis.name)
            if g is None:
                continue
            code = (pbp.instrument_code or "").strip().upper() or "—"
            lu = (
                pbp.last_updated.isoformat()
                if getattr(pbp, "last_updated", None)
                else None
            )
            out.append(
                (
                    g,
                    basis.name,
                    prod.name,
                    code,
                    float(pbp.current_price),
                    lu,
                )
            )
    order = {k: i for i, k in enumerate(GROUP_ORDER)}
    out.sort(key=lambda r: (order.get(r[0], 99), r[1], r[2]))
    return out


def print_report(
    rows: Sequence[Tuple[str, str, str, str, float, Optional[str]]],
) -> None:
    if not rows:
        print(
            "Нет строк: базисы не совпали с фильтрами (проверь точные name в таблице basis) "
            "или нет активных product_basis_prices."
        )
        return

    def _print_block(
        title: str,
        block: Sequence[Tuple[str, str, str, str, float, Optional[str]]],
    ) -> None:
        if not block:
            return
        print(f"\n  — {title} ({len(block)}) —")
        for g, bname, pname, code, price, lu in block:
            lu_s = lu or "—"
            print(
                f"  {pname[:50]:<50}  {code:14}  {price:>12,.0f} ₽/т  updated {lu_s}"
            )
            print(f"     базис: {bname}")

    order = {k: i for i, k in enumerate(GROUP_ORDER)}
    by_group: Dict[str, List[Tuple[str, str, str, str, float, Optional[str]]]] = {}
    for r in rows:
        by_group.setdefault(r[0], []).append(r)

    for gkey in sorted(by_group.keys(), key=lambda k: order.get(k, 99)):
        group_rows = by_group[gkey]
        with_price = [r for r in group_rows if r[4] > 0]
        zero_price = [r for r in group_rows if r[4] <= 0]

        print(f"\n=== {gkey} ===")
        _print_block("есть цена (> 0)", with_price)
        _print_block(
            "⚠ цена 0 ₽/т (строка в каталоге есть, из PDF не обновлено или не было «Рыночной»)",
            zero_price,
        )

    n_zero = sum(1 for r in rows if r[4] <= 0)
    print(
        f"\nИтого записей: {len(rows)} "
        f"(с ценой > 0: {len(rows) - n_zero}, с 0 ₽: {n_zero}; "
        f"уникальных базисов: {len({r[1] for r in rows})})"
    )
    print(
        "\nПодсказка:\n"
        "  • Если много нулей после импорта с --strict-market-column — в бюллетене у этих "
        "кодов часто пустая «Рыночная». Запусти импорт БЕЗ этого флага — подставятся "
        "средневзвешенная / макс / мин / лучшее предложение (см. import_spimex_prices_from_pdf.py).\n"
        "  • Нет строки продукта (ДТ-Л, ДТ-А, …) — добавь код в «Базисы поставок.xlsx» и "
        "python3 import_exchange_data.py --instruments …"
    )


def write_csv(
    path: str,
    rows: Sequence[Tuple[str, str, str, str, float, Optional[str]]],
) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(
            [
                "group",
                "basis_name",
                "product",
                "instrument_code",
                "current_price",
                "last_updated",
            ]
        )
        for r in rows:
            w.writerow(list(r))


async def list_all_basis_names_hint() -> None:
    async with AsyncSessionLocal() as session:
        q = await session.execute(
            select(Basis.name).where(Basis.is_active.is_(True)).order_by(Basis.name)
        )
        names = [x[0] for x in q.all()]
    keys = ("дзем", "ангарск", "кнпз")
    print("Подсказка: фрагменты активных базисов (дзем / ангарск / кнпз):\n")
    for n in names:
        low = _norm(n)
        if any(k in low for k in keys):
            print(f"  • {n}")


async def main_async(args: argparse.Namespace) -> int:
    rows = await fetch_rows()
    if args.list_basis_hints:
        await list_all_basis_names_hint()
        return 0
    print_report(rows)
    if args.csv:
        write_csv(args.csv, rows)
        print(f"\nCSV: {args.csv}")
    return 0


def main() -> None:
    p = argparse.ArgumentParser(
        description="Рыночные цены: Ангарск-группа станций, СН КНПЗ, Дзёмги"
    )
    p.add_argument(
        "--csv",
        metavar="FILE",
        help="Дополнительно сохранить отчёт в CSV (; UTF-8)",
    )
    p.add_argument(
        "--list-basis-hints",
        action="store_true",
        help="Показать активные базисы, где встречаются дзем/ангарск/кнпз",
    )
    args = p.parse_args()
    raise SystemExit(asyncio.run(main_async(args)))


if __name__ == "__main__":
    main()
