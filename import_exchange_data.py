#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Связка справочника инструментов (Базисы поставок.xlsx) и координат (баба.xlsx).

Порядок:
  1) --baba  — координаты, transport, код ЕСР → таблица basis
  2) --instruments — code, fuel_type, basis, transport → products + product_basis_prices

Пример:
  python3 import_exchange_data.py \\
    --baba "/Users/macbookair/Desktop/баба.xlsx" \\
    --instruments "/Users/macbookair/Desktop/Базисы поставок.xlsx"
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import warnings
from pathlib import Path

# Предупреждение pandas про pyarrow при импорте
warnings.filterwarnings(
    "ignore",
    message=".*Pyarrow will become a required dependency.*",
    category=DeprecationWarning,
)

import pandas as pd
from sqlalchemy import select

from import_geo_data import (
    _pick,
    _rename_df,
    _transport,
    clean_excel_text,
    update_basises_from_df,
)
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parent

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _resolve_xlsx(user_path: str, fallback_names: tuple[str, ...]) -> tuple[Path | None, list[str]]:
    """
    Путь как ввёл пользователь (~, относительный), затем ./ и папка проекта и Рабочий стол.
    Возвращает (файл или None, список проверенных путей для сообщения об ошибке).
    """
    tried: list[str] = []
    p = Path(user_path).expanduser()
    candidates = [p.resolve() if p.is_absolute() else (Path.cwd() / p).resolve()]
    for name in fallback_names:
        candidates.extend(
            [
                (ROOT / name).resolve(),
                (Path.home() / "Desktop" / name).resolve(),
                (Path.cwd() / name).resolve(),
            ]
        )
    seen: set[str] = set()
    for c in candidates:
        s = str(c)
        if s in seen:
            continue
        seen.add(s)
        tried.append(s)
        if c.is_file():
            return c, tried
    return None, tried


async def sync_instruments_catalog(session, df: pd.DataFrame) -> tuple[int, int]:
    """Строки Excel: code, fuel_type, basis, transport[, notes]."""
    from db.database import Basis, Product, ProductBasisPrice

    df = _rename_df(df)
    ok, skipped = 0, 0
    for _, raw in df.iterrows():
        row = raw.to_dict()
        code = _pick(row, "code", "код", "instrument_code", "код_инструмента")
        fuel = _pick(row, "fuel_type", "fuel", "product", "вид", "топливо")
        basis_name = _pick(row, "basis", "базис", "базис_поставки")
        transp = _transport(_pick(row, "transport", "transport_type", "тип", "транспорт"))

        if not code or not fuel or not basis_name:
            skipped += 1
            continue

        code_s = clean_excel_text(code).upper()
        basis_s = clean_excel_text(basis_name)
        fuel_s = clean_excel_text(fuel)
        if not code_s or not basis_s or not fuel_s:
            skipped += 1
            continue

        pr = (
            await session.execute(select(Product).where(Product.name == fuel_s))
        ).scalar_one_or_none()
        if not pr:
            pr = Product(name=fuel_s, is_active=True)
            session.add(pr)
            await session.flush()

        bs = (
            await session.execute(select(Basis).where(Basis.name == basis_s))
        ).scalar_one_or_none()
        if not bs:
            q_all = await session.execute(select(Basis))
            for cand in q_all.scalars().all():
                if clean_excel_text(cand.name) == basis_s:
                    bs = cand
                    break
        if not bs:
            logger.warning("Базис «%s» не найден в БД — сначала загрузите баба.xlsx", basis_s)
            skipped += 1
            continue

        if bs.transport_type != transp:
            logger.warning(
                "Транспорт в каталоге (%s) ≠ в БД (%s) для базиса «%s» — оставляем БД из баба.xlsx",
                transp,
                bs.transport_type,
                basis_s,
            )

        pbp = (
            await session.execute(
                select(ProductBasisPrice).where(ProductBasisPrice.instrument_code == code_s)
            )
        ).scalar_one_or_none()

        if not pbp:
            session.add(
                ProductBasisPrice(
                    instrument_code=code_s,
                    product_id=pr.id,
                    basis_id=bs.id,
                    current_price=0.0,
                    is_active=True,
                )
            )
        else:
            pbp.product_id = pr.id
            pbp.basis_id = bs.id
            pbp.is_active = True

        ok += 1

    await session.commit()
    return ok, skipped


async def main_async(args: argparse.Namespace) -> None:
    from db.database import AsyncSessionLocal

    try:
        from db.migrate_sqlite_rail import apply_basis_sqlite_migrations

        if apply_basis_sqlite_migrations():
            logger.info("Проверка SQLite: колонки basis (rail_*) при необходимости добавлены.")
    except Exception as e:
        logger.warning("Миграция колонок basis: %s", e)

    async with AsyncSessionLocal() as session:
        if args.baba:
            p, tried = _resolve_xlsx(args.baba, ("баба.xlsx", "baba.xlsx"))
            if p is None:
                print("⚠️ Файл баба НЕ найден. Проверьте путь и имя (латиница/кириллица).")
                print("   Искали:")
                for t in tried[:12]:
                    print(f"   • {t}")
                if len(tried) > 12:
                    print(f"   • … всего вариантов: {len(tried)}")
                print(
                    "\n   Подсказка: положите файл в папку fuel_bot и выполните:\n"
                    "   python3 import_exchange_data.py --baba ./баба.xlsx --instruments ./Базисы\\ поставок.xlsx\n"
                    "   или укажите точный путь из Finder: файл → зажать Option → «Скопировать … как путь»."
                )
            else:
                if str(p) != str(Path(args.baba).expanduser()):
                    print(f"ℹ️ баба: используется файл {p}")
                df = pd.read_excel(p)
                n = await update_basises_from_df(session, df)
                print(f"✅ баба.xlsx → basis: строк обработано {n}")

        if args.instruments:
            p, tried = _resolve_xlsx(args.instruments, ("Базисы поставок.xlsx",))
            if p is None:
                raise SystemExit(
                    "Нет файла «Базисы поставок». Проверьте путь.\nИскали:\n"
                    + "\n".join(f"  • {t}" for t in tried[:15])
                )
            if str(p) != str(Path(args.instruments).expanduser()):
                print(f"ℹ️ инструменты: используется файл {p}")
            df = pd.read_excel(p)
            ok, sk = await sync_instruments_catalog(session, df)
            print(f"✅ Базисы поставок.xlsx → инструменты: ok={ok}, пропуск={sk}")
            if ok == 0 and sk > 0:
                print(
                    "⚠️ Ни одной строки не загрузили: чаще всего нет базисов в таблице basis "
                    "(сначала успешно загрузите баба.xlsx)."
                )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Импорт баба.xlsx + Базисы поставок.xlsx")
    p.add_argument("--baba", default="", help="Путь к баба.xlsx (basis:, широта:, долгота:, transport, код еср:)")
    p.add_argument(
        "--instruments",
        default="",
        help="Путь к Базисы поставок.xlsx (code, fuel_type, basis, transport, notes)",
    )
    return p


def main() -> None:
    args = build_parser().parse_args()
    if not args.baba and not args.instruments:
        print("Укажите --baba и/или --instruments")
        raise SystemExit(1)
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
