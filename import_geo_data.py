#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Импорт координат базисов и справочника ж/д станций в БД.

Примеры:
  python3 import_geo_data.py --seed-stations
  python3 import_geo_data.py --seed-stations --bases-xlsx "./Базисы с координатами.xlsx"
  python3 import_geo_data.py --cities-xlsx "./города.xlsx"
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import select
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
DEFAULT_SEED_STATIONS = ROOT / "data" / "rail_stations_seed.csv"


def _norm_col(c: str) -> str:
    """Нормализация заголовков Excel: пробелы, нижний регистр, убираем «:» в конце (basis:, широта:)."""
    s = str(c).strip().lower().replace("\n", " ")
    s = re.sub(r"\s+", " ", s)
    s = s.rstrip(":").strip()
    return s


def _rename_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_norm_col(c) for c in df.columns]
    return df


def _pick(row: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in row and row[k] is not None and str(row[k]).strip() != "":
            return row[k]
    return None


def clean_excel_text(v: Any) -> str:
    """Убирает переносы строк в ячейках Excel (например «Ангарск-группа\\nстанций»)."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).replace("\r\n", "\n").replace("\r", "\n")
    s = " ".join(s.split())
    return s.strip()


def _float(v: Any) -> Optional[float]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _transport(val: Any) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "rail"
    s = str(val).strip().lower()
    if s in ("auto", "авто", "a", "road"):
        return "auto"
    if s in ("rail", "жд", "ж/д", "rzd", "train"):
        return "rail"
    return "rail"


async def upsert_rail_stations(session, df: pd.DataFrame, replace_all: bool = False) -> int:
    from sqlalchemy import delete

    from db.database import RailStation

    if replace_all:
        await session.execute(delete(RailStation))

    df = _rename_df(df)
    n = 0
    for _, raw in df.iterrows():
        row = raw.to_dict()
        name = _pick(row, "name", "станция", "station", "название", "st_name")
        if name is None:
            continue
        lat = _float(_pick(row, "latitude", "широта", "lat"))
        lon = _float(_pick(row, "longitude", "долгота", "lon", "lng"))
        if lat is None or lon is None:
            continue
        esr = _pick(row, "esr_code", "esr", "еср", "код")
        settlement = _pick(row, "settlement_name", "город", "нп", "населенный_пункт", "city")
        region = _pick(row, "region", "регион", "область")

        esr_s = str(esr).strip() if esr is not None else None
        if esr_s == "" or esr_s.lower() == "nan":
            esr_s = None

        q = await session.execute(select(RailStation).where(RailStation.name == str(name).strip()))
        existing = q.scalar_one_or_none()
        if existing:
            existing.latitude = lat
            existing.longitude = lon
            existing.esr_code = esr_s
            existing.settlement_name = str(settlement).strip() if settlement else None
            existing.region = str(region).strip() if region else None
            existing.is_active = True
        else:
            session.add(
                RailStation(
                    name=str(name).strip(),
                    esr_code=esr_s,
                    latitude=lat,
                    longitude=lon,
                    settlement_name=str(settlement).strip() if settlement else None,
                    region=str(region).strip() if region else None,
                    is_active=True,
                )
            )
        n += 1
    await session.commit()
    return n


async def update_basises_from_df(session, df: pd.DataFrame) -> int:
    from db.database import Basis

    # Старая SQLite-БД без колонок Ж/Д — добавляем их до первого SELECT по Basis
    try:
        from db.migrate_sqlite_rail import apply_basis_sqlite_migrations

        if apply_basis_sqlite_migrations():
            pass  # колонки есть или только что добавлены
    except Exception as e:
        logger.warning("Не удалось применить миграцию basis: %s", e)

    df = _rename_df(df)
    updated = 0
    for _, raw in df.iterrows():
        row = raw.to_dict()
        name_raw = _pick(row, "basis", "name", "базис", "название", "basis_name")
        if name_raw is None:
            continue
        name = clean_excel_text(name_raw)
        if not name:
            continue
        city = clean_excel_text(_pick(row, "city", "город") or "")
        lat = _float(_pick(row, "широта", "latitude", "lat"))
        lon = _float(_pick(row, "долгота", "longitude", "lon", "lng"))
        transport = _transport(_pick(row, "transport", "transport_type", "тип", "транспорт"))
        rname = _pick(row, "rail_station", "станция", "station", "станция_жд", "жд_станция")
        resr = _pick(row, "код еср", "код_еср", "rail_esr", "esr", "еср")
        rlat = _float(
            _pick(row, "rail_latitude", "rail_lat", "широта_ст", "станция_широта")
        )
        rlon = _float(
            _pick(row, "rail_longitude", "rail_lon", "долгота_ст", "станция_долгота")
        )

        q = await session.execute(select(Basis).where(Basis.name == name))
        b = q.scalar_one_or_none()
        if not b:
            q_all = await session.execute(select(Basis))
            for cand in q_all.scalars().all():
                if clean_excel_text(cand.name) == name:
                    b = cand
                    if cand.name != name:
                        cand.name = name
                    break

        if not b:
            # создаём новый базис-черновик (цены потом из другого импорта)
            b = Basis(
                name=name,
                city=city or name,
                latitude=lat or 0.0,
                longitude=lon or 0.0,
                is_active=True,
                transport_type=transport,
            )
            session.add(b)
            await session.flush()
        else:
            if lat is not None:
                b.latitude = lat
            if lon is not None:
                b.longitude = lon
            b.transport_type = transport
            if city:
                b.city = city

        if rname:
            b.rail_station_name = clean_excel_text(rname)
        if resr:
            b.rail_esr = str(resr).strip().replace(" ", "")
        if rlat is not None:
            b.rail_latitude = rlat
        if rlon is not None:
            b.rail_longitude = rlon

        # баба.xlsx: координаты группы станций — для Ж/Д продублируем в rail_* если пусто
        if transport == "rail" and lat is not None and lon is not None:
            if b.rail_latitude is None:
                b.rail_latitude = lat
            if b.rail_longitude is None:
                b.rail_longitude = lon
            if not b.rail_station_name:
                b.rail_station_name = name

        updated += 1

    await session.commit()
    return updated


async def upsert_city_destinations(session, df: pd.DataFrame) -> int:
    from db.database import CityDestination
    from utils import normalize_city_name_key

    df = _rename_df(df)
    n = 0
    for _, raw in df.iterrows():
        row = raw.to_dict()
        name = _pick(row, "name", "город", "населенный_пункт", "нп", "city")
        if name is None:
            continue
        lat = _float(_pick(row, "latitude", "широта", "lat"))
        lon = _float(_pick(row, "longitude", "долгота", "lon", "lng"))
        if lat is None or lon is None:
            continue
        region = _pick(row, "region", "регион")

        name_clean = str(name).replace("ё", "е").replace("Ё", "Е").strip()
        key = normalize_city_name_key(name_clean)

        q = await session.execute(
            select(CityDestination).where(
                CityDestination.name == name_clean
            )
        )
        ex = q.scalar_one_or_none()
        if not ex:
            q2 = await session.execute(select(CityDestination))
            for c in q2.scalars().all():
                if normalize_city_name_key(c.name) == key:
                    ex = c
                    break

        if ex:
            ex.latitude = lat
            ex.longitude = lon
            ex.region = str(region).strip() if region else ex.region
        else:
            session.add(
                CityDestination(
                    name=name_clean,
                    region=str(region).strip() if region else None,
                    latitude=lat,
                    longitude=lon,
                )
            )
        n += 1
    await session.commit()
    return n


async def main_async(args: argparse.Namespace) -> None:
    from db.database import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        total_st = 0
        if args.seed_stations:
            path = Path(args.seed_csv or DEFAULT_SEED_STATIONS)
            if not path.is_file():
                raise SystemExit(f"Нет файла станций: {path}")
            df = pd.read_csv(path)
            total_st += await upsert_rail_stations(
                session, df, replace_all=bool(args.replace_stations)
            )
            print(f"✅ Станции (seed): обработано строк: {total_st}")

        if args.stations_xlsx:
            df = pd.read_excel(args.stations_xlsx)
            n = await upsert_rail_stations(session, df, replace_all=False)
            print(f"✅ Станции (xlsx): обновлено/добавлено строк: {n}")

        if args.bases_xlsx:
            df = pd.read_excel(args.bases_xlsx)
            n = await update_basises_from_df(session, df)
            print(f"✅ Базисы (xlsx): обработано строк: {n}")

        if args.cities_xlsx:
            df = pd.read_excel(args.cities_xlsx)
            n = await upsert_city_destinations(session, df)
            print(f"✅ Населённые пункты (xlsx): строк: {n}")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Импорт геоданных в fuel_bot")
    p.add_argument("--seed-stations", action="store_true", help="Загрузить data/rail_stations_seed.csv")
    p.add_argument("--seed-csv", default="", help="Другой CSV со станциями")
    p.add_argument(
        "--replace-stations",
        action="store_true",
        help="Перед загрузкой очистить таблицу rail_stations",
    )
    p.add_argument("--stations-xlsx", default="", help="Excel со станциями")
    p.add_argument("--bases-xlsx", default="", help="Excel с базисами и координатами")
    p.add_argument("--cities-xlsx", default="", help="Excel с городами назначения (city_destinations)")
    return p


def main() -> None:
    args = build_arg_parser().parse_args()
    if not any(
        [
            args.seed_stations,
            args.stations_xlsx,
            args.bases_xlsx,
            args.cities_xlsx,
        ]
    ):
        print("Укажите хотя бы один флаг: --seed-stations, --stations-xlsx, --bases-xlsx, --cities-xlsx")
        raise SystemExit(1)
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
