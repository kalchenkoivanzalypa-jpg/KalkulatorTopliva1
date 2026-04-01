#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Импорт rail_stations из Книги 2 (алфавитный список станций) с дедупликацией.

Источник:
  railway/data/kniga2/Kniga_2_2026-03-12.csv

Логика:
  1) Читаем станции и коды ЕСР из Книги 2
  2) Нормализуем названия станций/поселений
  3) Подтягиваем координаты из city_destinations (по settlement_name) и rail_stations_seed
  4) Загружаем в rail_stations через upsert_rail_stations (с optional replace)
  5) Печатаем проверку покрытия по ключевым городам + заданным пользователем
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.database import AsyncSessionLocal, Basis, CityDestination, RailStation
from import_geo_data import upsert_rail_stations
from utils import normalize_city_name_key

DEFAULT_KNIGA2 = ROOT / "railway" / "data" / "kniga2" / "Kniga_2_2026-03-12.csv"
DEFAULT_SEED = ROOT / "data" / "rail_stations_seed.csv"
DEFAULT_OUT = ROOT / "data" / "rail_stations_from_kniga2.csv"


def _clean_text(v: object) -> str:
    s = str(v or "").strip().replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s)
    return s


def _clean_station_name(name: str) -> str:
    s = _clean_text(name)
    s = s.strip(",.;:")
    return s


def _extract_esr(raw: str) -> Optional[str]:
    m = re.search(r"\b(\d{6})\b", raw or "")
    if m:
        return m.group(1)
    return None


def _guess_settlement(station_name: str) -> str:
    s = _clean_station_name(station_name)
    # Убираем типовые суффиксы, чтобы лучше матчилось с city_destinations
    s = re.sub(r"\s+\(.*?\)\s*$", "", s).strip()
    s = re.sub(r"\b(пассажирск(?:ий|ая)?|сортировочн(?:ая|ый)|главн(?:ый|ая))\b", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip(" -")
    return s or station_name


def _load_seed_coords(path: Path) -> Dict[str, Tuple[float, float, str]]:
    out: Dict[str, Tuple[float, float, str]] = {}
    if not path.is_file():
        return out
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = _clean_station_name(row.get("name", ""))
            lat = row.get("latitude")
            lon = row.get("longitude")
            if not name or not lat or not lon:
                continue
            try:
                lat_f = float(str(lat).replace(",", "."))
                lon_f = float(str(lon).replace(",", "."))
            except ValueError:
                continue
            settlement = _clean_text(row.get("settlement_name", "")) or _guess_settlement(name)
            out[name.lower()] = (lat_f, lon_f, settlement)
    return out


async def _load_city_coords() -> Dict[str, Tuple[float, float, str]]:
    out: Dict[str, Tuple[float, float, str]] = {}
    async with AsyncSessionLocal() as session:
        q = await session.execute(select(CityDestination.name, CityDestination.latitude, CityDestination.longitude))
        for name, lat, lon in q.all():
            if name is None:
                continue
            key = normalize_city_name_key(str(name))
            out[key] = (float(lat), float(lon), str(name))
    return out


async def _load_basis_esr_coords() -> Dict[str, Tuple[float, float, str]]:
    """
    Карта ESR -> (lat, lon, settlement_name) из basis.
    Если у базиса есть rail_esr и rail-координаты, это наиболее надёжный источник
    для станций отправления, привязанных к базисам.
    """
    out: Dict[str, Tuple[float, float, str]] = {}
    async with AsyncSessionLocal() as session:
        q = await session.execute(
            select(
                Basis.rail_esr,
                Basis.rail_latitude,
                Basis.rail_longitude,
                Basis.city,
            ).where(Basis.is_active.is_(True))
        )
        for esr, lat, lon, city in q.all():
            if esr is None or lat is None or lon is None:
                continue
            esr_key = str(esr).strip().replace(" ", "").replace(".0", "")
            if not esr_key.isdigit():
                continue
            out[esr_key] = (float(lat), float(lon), _clean_text(city or ""))
    return out


def _pick_coords(
    station_name: str,
    settlement_guess: str,
    city_coords: Dict[str, Tuple[float, float, str]],
    seed_coords: Dict[str, Tuple[float, float, str]],
) -> Optional[Tuple[float, float, str]]:
    # 1) exact station match in seed
    by_station = seed_coords.get(station_name.lower())
    if by_station:
        return by_station

    # 2) city_destinations exact by normalized settlement
    key = normalize_city_name_key(settlement_guess)
    if key in city_coords:
        lat, lon, city_name = city_coords[key]
        return (lat, lon, city_name)

    # 3) Лёгкий фоллбек по "базовой" форме (первое слово / без суффиксов)
    # Избегаем O(N*M) перебора по всем городам.
    base = key.split(" ")[0] if key else ""
    if base and base in city_coords:
        lat, lon, city_name = city_coords[base]
        return (lat, lon, city_name)
    return None


def _parse_kniga2_csv(path: Path) -> List[Tuple[str, str]]:
    """
    Возвращает список (station_name, esr_code) из CSV Книги 2.
    В файле шапка не первая строка, поэтому ищем строку с названиями колонок.
    """
    rows: List[List[str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    header_idx = None
    for i, r in enumerate(rows):
        joined = " | ".join(_clean_text(x) for x in r)
        if "Наименование пункта" in joined and "Код станции" in joined:
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("Не найдена строка заголовков с 'Наименование пункта' / 'Код станции'")

    header = [(_clean_text(x) or f"col_{j}") for j, x in enumerate(rows[header_idx])]
    c_name_idx = next((j for j, c in enumerate(header) if "Наименование пункта" in c), None)
    c_code_idx = next((j for j, c in enumerate(header) if "Код станции" in c), None)
    if c_name_idx is None or c_code_idx is None:
        raise RuntimeError("Не определились индексы колонок имени/кода")

    out: List[Tuple[str, str]] = []
    for r in rows[header_idx + 1 :]:
        if not r:
            continue
        raw_name = _clean_station_name(r[c_name_idx] if c_name_idx < len(r) else "")
        raw_code = _clean_text(r[c_code_idx] if c_code_idx < len(r) else "")
        if not raw_name:
            continue
        esr = _extract_esr(raw_code)
        if not esr:
            continue
        out.append((raw_name, esr))
    return out


async def build_dataset(kniga2_path: Path, seed_path: Path) -> pd.DataFrame:
    pairs = _parse_kniga2_csv(kniga2_path)
    city_coords = await _load_city_coords()
    seed_coords = _load_seed_coords(seed_path)
    basis_esr_coords = await _load_basis_esr_coords()

    by_esr: Dict[str, Dict[str, object]] = {}
    for station_name, esr in pairs:
        # 0) Приоритет: если ESR уже используется в basis с rail-координатами
        # (как Суховская=932207 для Ангарска), берём именно эти координаты.
        basis_pick = basis_esr_coords.get(esr)
        if basis_pick is not None:
            lat, lon, settlement_name = basis_pick
            if esr not in by_esr:
                by_esr[esr] = {
                    "name": station_name,
                    "esr_code": esr,
                    "settlement_name": settlement_name or _guess_settlement(station_name),
                    "latitude": lat,
                    "longitude": lon,
                    "region": None,
                }
            continue

        settlement = _guess_settlement(station_name)
        pick = _pick_coords(station_name, settlement, city_coords, seed_coords)
        if not pick:
            continue
        lat, lon, settlement_name = pick
        # дедуп по ЕСР: если повторяется, оставляем первый
        if esr in by_esr:
            continue
        by_esr[esr] = {
            "name": station_name,
            "esr_code": esr,
            "settlement_name": settlement_name,
            "latitude": lat,
            "longitude": lon,
            "region": None,
        }

    rows = list(by_esr.values())
    out = pd.DataFrame(rows)
    return out


async def print_coverage(cities: List[str]) -> None:
    async with AsyncSessionLocal() as session:
        q = await session.execute(
            select(RailStation.name, RailStation.settlement_name, RailStation.esr_code)
            .where(RailStation.is_active.is_(True))
        )
        stations = q.all()

    print("\nПокрытие по городам:")
    for city in cities:
        key = normalize_city_name_key(city)
        matches = []
        for st_name, settlement, esr in stations:
            s_key = normalize_city_name_key(settlement or "")
            n_key = normalize_city_name_key(st_name or "")
            if key and (key in s_key or key == s_key or key in n_key):
                matches.append((st_name, settlement, esr))
        print(f"- {city}: {len(matches)}")
        for st_name, settlement, esr in matches[:5]:
            print(f"    • {st_name} | settlement={settlement} | esr={esr}")


async def main_async(args: argparse.Namespace) -> None:
    dataset = await build_dataset(Path(args.kniga2_csv), Path(args.seed_csv))
    if dataset.empty:
        raise SystemExit("Не собраны станции: проверьте входные файлы и city_destinations")

    out_csv = Path(args.out_csv)
    dataset.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"Собран CSV для импорта: {out_csv} (строк: {len(dataset)})")

    async with AsyncSessionLocal() as session:
        n = await upsert_rail_stations(
            session,
            dataset,
            replace_all=bool(args.replace_stations),
        )
    print(f"✅ Импорт rail_stations завершен: {n} строк")

    default_cities = [
        "Москва",
        "Санкт-Петербург",
        "Екатеринбург",
        "Новосибирск",
        "Казань",
        "Уфа",
        "Пурпе",
        "Тында",
        "Южно-Сахалинск",
    ]
    await print_coverage(default_cities)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Импорт rail_stations из Книга 2")
    p.add_argument("--kniga2-csv", default=str(DEFAULT_KNIGA2), help="Путь к CSV Книги 2")
    p.add_argument("--seed-csv", default=str(DEFAULT_SEED), help="Путь к seed CSV станций (координаты)")
    p.add_argument("--out-csv", default=str(DEFAULT_OUT), help="Куда сохранить промежуточный CSV")
    p.add_argument("--replace-stations", action="store_true", help="Очистить rail_stations перед импортом")
    return p


def main() -> None:
    args = build_parser().parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()

