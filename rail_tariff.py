# -*- coding: utf-8 -*-
"""
Расчёт доставки Ж/Д для финального экрана (вагоны + стоимость).
Расстояние для тарифа: Тарифное руководство №4 (если подключён модуль) иначе оценка по гео.

Ставка руб/(т·км): калибровка как в utils.get_delivery_rate_sync для rail.
"""
from __future__ import annotations

import importlib.util
import logging
import math
import os
import csv
import sqlite3
import sys
from functools import lru_cache
from pathlib import Path
from typing import Optional, TypedDict, Literal

from haversine import Unit, haversine

from utils import get_delivery_rate_sync

logger = logging.getLogger(__name__)

# Условная грузоподъёмность цистерны (т), для отображения в боте
DEFAULT_TONS_PER_WAGON = 60.0

# Насколько ж/д маршрут длиннее прямой (если нет ТР №4), типично 1.08–1.25
DEFAULT_RAIL_ROUTE_FACTOR = float(os.getenv("RAIL_ROUTE_FACTOR", "1.15"))
DEFAULT_RAIL_DELIVERY_MODE = os.getenv("RAIL_DELIVERY_MODE", "full").strip().lower()

_BENCHMARK_ROWS: list[dict[str, float]] | None = None


def _load_rail_benchmarks() -> list[dict[str, float]]:
    """
    Загружает эталонные кейсы РЖД из data/rail_rzd_benchmarks.csv.
    Используются поля tariff_distance_km и rate_per_ton_rub.
    """
    global _BENCHMARK_ROWS
    if _BENCHMARK_ROWS is not None:
        return _BENCHMARK_ROWS

    path = Path(__file__).resolve().parent / "data" / "rail_rzd_benchmarks.csv"
    rows: list[dict[str, float]] = []
    if not path.is_file():
        _BENCHMARK_ROWS = rows
        return rows

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                d = float(str(r.get("tariff_distance_km", "0")).replace(" ", "").replace(",", "."))
                rate_per_ton = float(str(r.get("rate_per_ton_rub", "0")).replace(" ", "").replace(",", "."))
                if d > 0 and rate_per_ton > 0:
                    rows.append(
                        {
                            "distance_km": d,
                            "rate_per_ton_rub": rate_per_ton,
                            "transportation_rub": float(str(r.get("transportation_rub", "0")).replace(" ", "").replace(",", ".")),
                            "security_rub": float(str(r.get("security_rub", "0")).replace(" ", "").replace(",", ".")),
                            "wagon_provision_rub": float(str(r.get("wagon_provision_rub", "0")).replace(" ", "").replace(",", ".")),
                        }
                    )
            except Exception:
                continue

    rows.sort(key=lambda x: x["distance_km"])
    _BENCHMARK_ROWS = rows
    return rows


def _interpolate_by_distance(distance_km: float, rows: list[dict[str, float]], key: str) -> float:
    if not rows:
        return 0.0
    d = float(distance_km)
    if d <= rows[0]["distance_km"]:
        return float(rows[0][key])
    if d >= rows[-1]["distance_km"]:
        return float(rows[-1][key])
    for i in range(1, len(rows)):
        a = rows[i - 1]
        b = rows[i]
        da = a["distance_km"]
        db = b["distance_km"]
        if da <= d <= db and db > da:
            t = (d - da) / (db - da)
            return float(a[key]) + t * (float(b[key]) - float(a[key]))
    return float(rows[-1][key])


def straight_line_km(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    return float(haversine((lat1, lon1), (lat2, lon2), unit=Unit.KILOMETERS))


def compute_rail_tariff_distance_km(
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
    origin_esr: Optional[str] = None,
    dest_esr: Optional[str] = None,
) -> float:
    """
    Километраж для применения ставки Ж/Д.

    1) Если задан RZD_TR4_MODULE (путь к .py), вызывается
       ``distance_km(origin_esr, dest_esr)`` или ``tariff4_distance(...)`` при наличии.
    2) Иначе пытаемся расчёт по локальным Книгам 2/3 (railway/logistics.py).
    3) Если и это не удалось: прямая между точками * DEFAULT_RAIL_ROUTE_FACTOR.
    """
    tr4_path = os.getenv("RZD_TR4_MODULE", "").strip()
    if tr4_path and origin_esr and dest_esr:
        try:
            spec = importlib.util.spec_from_file_location("rzd_tr4_dynamic", tr4_path)
            mod = importlib.util.module_from_spec(spec)
            assert spec and spec.loader
            spec.loader.exec_module(mod)  # type: ignore
            if hasattr(mod, "distance_km"):
                d = float(mod.distance_km(str(origin_esr), str(dest_esr)))
                if d > 0:
                    return d
            if hasattr(mod, "tariff4_distance"):
                d = float(mod.tariff4_distance(str(origin_esr), str(dest_esr)))
                if d > 0:
                    return d
        except Exception as exc:
            logger.warning("ТР №4 модуль не сработал (%s), используем гео-оценку", exc)

    if origin_esr and dest_esr:
        try:
            d = _distance_from_local_tariff_books(str(origin_esr), str(dest_esr))
            if d > 0:
                return d
        except Exception as exc:
            logger.warning("Локальные Книги 2/3 не сработали (%s), используем гео-оценку", exc)

    base = straight_line_km(origin_lat, origin_lon, dest_lat, dest_lon)
    return max(1.0, base * DEFAULT_RAIL_ROUTE_FACTOR)


class RailDistanceDebug(TypedDict, total=False):
    distance_km: float
    source: Literal["tr4_module", "local_books", "geo_fallback"]
    origin_esr: str
    dest_esr: str
    origin_lat: float
    origin_lon: float
    dest_lat: float
    dest_lon: float
    straight_km: float
    route_factor: float
    error: str


def compute_rail_tariff_distance_debug(
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
    origin_esr: Optional[str] = None,
    dest_esr: Optional[str] = None,
) -> RailDistanceDebug:
    """
    То же что compute_rail_tariff_distance_km, но возвращает источник и детали,
    чтобы можно было показать «почему так» в боте.
    """
    dbg: RailDistanceDebug = {
        "origin_lat": float(origin_lat),
        "origin_lon": float(origin_lon),
        "dest_lat": float(dest_lat),
        "dest_lon": float(dest_lon),
        "route_factor": float(DEFAULT_RAIL_ROUTE_FACTOR),
    }
    if origin_esr:
        dbg["origin_esr"] = str(origin_esr)
    if dest_esr:
        dbg["dest_esr"] = str(dest_esr)

    tr4_path = os.getenv("RZD_TR4_MODULE", "").strip()
    if tr4_path and origin_esr and dest_esr:
        try:
            spec = importlib.util.spec_from_file_location("rzd_tr4_dynamic", tr4_path)
            mod = importlib.util.module_from_spec(spec)
            assert spec and spec.loader
            spec.loader.exec_module(mod)  # type: ignore
            if hasattr(mod, "distance_km"):
                d = float(mod.distance_km(str(origin_esr), str(dest_esr)))
                if d > 0:
                    dbg["distance_km"] = d
                    dbg["source"] = "tr4_module"
                    return dbg
            if hasattr(mod, "tariff4_distance"):
                d = float(mod.tariff4_distance(str(origin_esr), str(dest_esr)))
                if d > 0:
                    dbg["distance_km"] = d
                    dbg["source"] = "tr4_module"
                    return dbg
        except Exception as exc:
            dbg["error"] = f"TR4: {exc}"

    if origin_esr and dest_esr:
        try:
            d = _distance_from_local_tariff_books(str(origin_esr), str(dest_esr))
            if d > 0:
                dbg["distance_km"] = d
                dbg["source"] = "local_books"
                return dbg
        except Exception as exc:
            dbg["error"] = (dbg.get("error", "") + f"; books: {exc}").strip("; ")

    straight = straight_line_km(origin_lat, origin_lon, dest_lat, dest_lon)
    dbg["straight_km"] = float(straight)
    dbg["distance_km"] = max(1.0, straight * DEFAULT_RAIL_ROUTE_FACTOR)
    dbg["source"] = "geo_fallback"
    return dbg


@lru_cache(maxsize=1)
def _sqlite_db_path() -> Optional[str]:
    """
    Абсолютный путь к SQLite-файлу из DATABASE_URL.
    """
    try:
        from sqlalchemy.engine.url import make_url
    except Exception:
        return None
    raw = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///fuel_bot.db")
    try:
        u = make_url(raw)
    except Exception:
        return None
    if "sqlite" not in u.drivername or not u.database:
        return None
    p = u.database
    if not os.path.isabs(p):
        p = os.path.abspath(p)
    return p


@lru_cache(maxsize=8192)
def _station_name_by_esr(esr: str) -> Optional[str]:
    db_path = _sqlite_db_path()
    if not db_path or not os.path.isfile(db_path):
        return None
    # ESR в БД иногда попадает как "932207.0" (из Excel/CSV). Нормализуем.
    key = str(esr).strip().replace(" ", "").replace(".0", "")
    if not key:
        return None
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        # rail_stations.esr_code может быть и "932207", и "932207.0"
        cur.execute(
            """
            SELECT name
            FROM rail_stations
            WHERE is_active = 1
              AND (
                REPLACE(REPLACE(COALESCE(esr_code,''), '.0', ''), ' ', '') = ?
              )
            LIMIT 1
            """,
            (key,),
        )
        row = cur.fetchone()
        return str(row[0]).strip() if row and row[0] else None
    finally:
        conn.close()


@lru_cache(maxsize=4096)
def _distance_from_local_tariff_books(origin_esr: str, dest_esr: str) -> float:
    """
    Расчёт расстояния по локальным Книгам 2/3 через railway/logistics.py.
    Нужен как второй этап до гео-fallback.
    """
    origin_name = _station_name_by_esr(origin_esr)
    dest_name = _station_name_by_esr(dest_esr)
    if not origin_name or not dest_name:
        raise ValueError("Не удалось сопоставить ESR со станциями")

    module_path = Path(__file__).resolve().parent / "railway" / "logistics.py"
    if not module_path.is_file():
        raise FileNotFoundError(f"Нет модуля логистики: {module_path}")

    railway_dir = str(module_path.parent)
    if railway_dir not in sys.path:
        sys.path.insert(0, railway_dir)
    mod_name = "railway_logistics_dynamic"
    spec = importlib.util.spec_from_file_location(mod_name, str(module_path))
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)  # type: ignore

    if not hasattr(mod, "logistic_distance_km"):
        raise AttributeError("В railway/logistics.py нет logistic_distance_km")

    data_root = str((Path(__file__).resolve().parent / "railway" / "data"))
    d = float(mod.logistic_distance_km(origin_name, dest_name, data_root=data_root))
    if d <= 0:
        raise ValueError("Локальный тарифный расчёт вернул неположительное расстояние")
    return d


def get_rail_rate(distance_km: float) -> float:
    """Ставка руб/(т·км) для ж/д по дистанции (как в utils для rail)."""
    return float(get_delivery_rate_sync(float(distance_km), "rail"))


def calculate_delivery_cost(distance_km: float, volume_tonns: float) -> dict:
    """
    Полная стоимость доставки Ж/Д и метаданные для сообщения пользователю.

    Возвращает ключи, которые ожидает bot/handlers.calculate_final_result:
    total_cost, rate_per_ton_km, wagons_needed, tons_per_wagon
    """
    d = float(distance_km)
    v = float(volume_tonns)
    if v <= 0:
        raise ValueError("Объём должен быть положительным")

    mode = DEFAULT_RAIL_DELIVERY_MODE
    if mode == "full":
        rows = _load_rail_benchmarks()
        if rows:
            # В full-режиме интерполируем ставку за тонну по реальным кейсам РЖД.
            rate_per_ton = _interpolate_by_distance(d, rows, "rate_per_ton_rub")
            total_cost = rate_per_ton * v
            rate = max(0.0001, rate_per_ton / max(1.0, d))

            # Справочно: приблизительная декомпозиция (для дальнейших доработок UI/отчета).
            transportation = _interpolate_by_distance(d, rows, "transportation_rub") * (v / DEFAULT_TONS_PER_WAGON)
            security = _interpolate_by_distance(d, rows, "security_rub") * (v / DEFAULT_TONS_PER_WAGON)
            wagon_provision = _interpolate_by_distance(d, rows, "wagon_provision_rub") * (v / DEFAULT_TONS_PER_WAGON)
        else:
            rate = get_rail_rate(d)
            total_cost = d * v * rate
            transportation = total_cost
            security = 0.0
            wagon_provision = 0.0
    else:
        # base: историческая формула руб/(т*км)
        rate = get_rail_rate(d)
        total_cost = d * v * rate
        transportation = total_cost
        security = 0.0
        wagon_provision = 0.0
    wagons = max(1, math.ceil(v / DEFAULT_TONS_PER_WAGON))

    return {
        "distance_km": round(d, 1),
        "volume_tonns": v,
        "rate_per_ton_km": round(rate, 4),
        "total_cost": round(total_cost, 2),
        "cost_per_ton": round(total_cost / v, 2),
        "wagons_needed": wagons,
        "tons_per_wagon": int(DEFAULT_TONS_PER_WAGON),
        "delivery_mode": mode,
        "components": {
            "transportation_rub": round(float(transportation), 2),
            "security_rub": round(float(security), 2),
            "wagon_provision_rub": round(float(wagon_provision), 2),
        },
    }


__all__ = [
    "calculate_delivery_cost",
    "compute_rail_tariff_distance_km",
    "get_rail_rate",
    "straight_line_km",
    "DEFAULT_TONS_PER_WAGON",
    "DEFAULT_RAIL_ROUTE_FACTOR",
]
