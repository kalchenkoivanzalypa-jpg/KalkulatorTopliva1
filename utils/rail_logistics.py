# -*- coding: utf-8 -*-
"""
Логистика Ж/Д: ближайшая станция к точке назначения, координаты «отправления» у базиса.
"""
from __future__ import annotations

import logging
import os
import re
from typing import Optional, Tuple

from haversine import Unit, haversine
from sqlalchemy import select

logger = logging.getLogger(__name__)
SAKHALIN_FERRY_SURCHARGE_PER_TON = float(os.getenv("SAKHALIN_FERRY_SURCHARGE_PER_TON", "4239"))


def geo_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Расстояние по дуге большого круга, км."""
    return float(haversine((lat1, lon1), (lat2, lon2), unit=Unit.KILOMETERS))


async def nearest_rail_station_to_point(session, lat: float, lon: float):
    """
    Ближайшая активная станция из справочника rail_stations.
    Если справочник пуст — None (бот откатится к расчёту «по прямой» до координат города).
    """
    # Ленивый импорт, чтобы не было циклов
    from db.database import RailStation

    result = await session.execute(
        select(RailStation).where(RailStation.is_active.is_(True))
    )
    stations = result.scalars().all()
    if not stations:
        logger.warning("Справочник rail_stations пуст — Ж/Д расстояние будет оценочным")
        return None

    best = min(
        stations,
        key=lambda s: geo_distance_km(lat, lon, s.latitude, s.longitude),
    )
    return best


def basis_rail_origin_coords(basis) -> Tuple[float, float]:
    """
    Точка «отправления» для Ж/Д: rail_latitude/rail_longitude, иначе координаты базиса.
    """
    if getattr(basis, "rail_latitude", None) is not None and getattr(
        basis, "rail_longitude", None
    ) is not None:
        return float(basis.rail_latitude), float(basis.rail_longitude)
    return float(basis.latitude), float(basis.longitude)


def basis_rail_origin_label(basis) -> str:
    """Подпись станции отправления для пользователя."""
    name = getattr(basis, "rail_station_name", None) or getattr(basis, "name", "") or ""
    city = getattr(basis, "city", None) or ""
    if name.strip():
        return name.strip()
    return city.strip() or "базис"


async def find_rail_station_by_settlement_name(session, name_key: str):
    """
    Поиск станции по названию населённого пункта (нормализованный ключ).
    name_key — уже нормализованная строка (как normalize_city_name_key).
    """
    from db.database import RailStation
    from utils import normalize_city_name_key

    if not name_key:
        return None

    result = await session.execute(
        select(RailStation).where(RailStation.is_active.is_(True))
    )
    for st in result.scalars().all():
        if st.settlement_name and normalize_city_name_key(st.settlement_name) == name_key:
            return st
    return None


def _normalized_station_key(value: str) -> str:
    """Нормализация произвольного названия станции/поселения."""
    from utils import normalize_city_name_key

    s = normalize_city_name_key(value or "")
    # Частые префиксы у названий станций
    for prefix in ("ст. ", "станция ", "жд станция ", "ж/д станция "):
        if s.startswith(prefix):
            s = s[len(prefix) :].strip()
            break
    return s


async def find_rail_station_for_destination(
    session,
    destination_raw: str,
    destination_name_key: str,
):
    """
    Поиск станции для пользовательского ввода:
    1) точное совпадение по settlement_name
    2) точное совпадение по station.name
    3) нестрогое совпадение (подстрока) по station.name/settlement_name
    """
    from db.database import RailStation
    from utils import normalize_city_name_key

    key = _normalized_station_key(destination_raw or destination_name_key or "")
    if not key:
        return None

    result = await session.execute(
        select(RailStation).where(RailStation.is_active.is_(True))
    )
    stations = result.scalars().all()
    if not stations:
        return None

    # 1) Точное совпадение по населённому пункту
    for st in stations:
        if st.settlement_name and normalize_city_name_key(st.settlement_name) == key:
            return st

    # 2) Точное совпадение по названию станции
    for st in stations:
        if _normalized_station_key(st.name or "") == key:
            return st

    # 3) Нестрогое совпадение (созвучное/частичное)
    # Для коротких ключей (например "оха") избегаем совпадений "по подстроке"
    # внутри других слов (например "коханово").
    scored: list[tuple[int, object]] = []
    key_words = [w for w in key.split(" ") if w]
    for st in stations:
        st_name_key = _normalized_station_key(st.name or "")
        st_set_key = normalize_city_name_key(st.settlement_name or "")
        score = 0
        name_words = [w for w in st_name_key.split(" ") if w]
        set_words = [w for w in st_set_key.split(" ") if w]

        if key in st_name_key:
            if len(key.replace(" ", "")) <= 4 and key not in name_words:
                pass
            else:
                score = max(score, 220 if st_name_key.startswith(key) else 130)
        if key in st_set_key:
            if len(key.replace(" ", "")) <= 4 and key not in set_words:
                pass
            else:
                score = max(score, 210 if st_set_key.startswith(key) else 120)

        if key_words and all(w in name_words for w in key_words):
            score = max(score, 205)
        if key_words and all(w in set_words for w in key_words):
            score = max(score, 195)

        if st_name_key in key and st_name_key:
            score = max(score, 90)
        if st_set_key in key and st_set_key:
            score = max(score, 80)
        if score > 0:
            scored.append((score, st))

    if not scored:
        return None
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]


def is_sakhalin_station(station) -> bool:
    """Признак, что станция относится к Сахалинской области."""
    if station is None:
        return False
    vals = [
        getattr(station, "region", None),
        getattr(station, "name", None),
        getattr(station, "settlement_name", None),
    ]
    for v in vals:
        s = str(v or "").lower().replace("ё", "е")
        if "сахалин" in s:
            return True
        if any(
            k in s
            for k in (
                "южно-сахалинск",
                "корсаков",
                "холмск",
                "поронайск",
                "ноглики",
                "невельск",
                "долинск",
                "анива",
                "томари",
                "макаров",
                "углегорск",
                "оха",
            )
        ):
            return True
    esr = str(getattr(station, "esr_code", "") or "").replace(" ", "").replace(".0", "")
    if re.fullmatch(r"\d{6}", esr) and esr.startswith("99"):
        return True
    try:
        lat = float(getattr(station, "latitude", 0.0) or 0.0)
        lon = float(getattr(station, "longitude", 0.0) or 0.0)
        if is_sakhalin_geo_point(lat, lon):
            return True
    except Exception:
        pass
    return False


def is_sakhalin_geo_point(lat: float, lon: float) -> bool:
    """Грубый bbox острова Сахалин для фильтрации ошибочных координат."""
    return 45.0 <= float(lat) <= 55.0 and 141.0 <= float(lon) <= 146.0


def is_sakhalin_destination(
    destination_raw: str = "",
    destination_name_key: str = "",
    destination_station=None,
) -> bool:
    """
    Признак «назначение на Сахалине».
    Используем текст ввода + найденную станцию.
    """
    if is_sakhalin_station(destination_station):
        return True

    raw = (destination_raw or "").lower().replace("ё", "е")
    key = (destination_name_key or "").lower().replace("ё", "е")
    checks = [raw, key]
    for s in checks:
        if "сахалин" in s:
            return True
        if "южно-сахалинск" in s or "южно сахалинск" in s:
            return True
        if any(
            k in s
            for k in (
                "корсаков",
                "холмск",
                "поронайск",
                "ноглики",
                "невельск",
                "долинск",
                "анива",
                "томари",
                "макаров",
                "углегорск",
                "оха",
            )
        ):
            return True
    return False


def sakhalin_ferry_surcharge_per_ton(is_sakhalin: bool) -> float:
    """Доплата парома для Сахалина, руб/т."""
    return float(SAKHALIN_FERRY_SURCHARGE_PER_TON) if is_sakhalin else 0.0


def sakhalin_ferry_surcharge_total(volume_tons: float, is_sakhalin: bool) -> float:
    """Доплата парома для всего объема, руб."""
    if not is_sakhalin:
        return 0.0
    return float(volume_tons) * float(SAKHALIN_FERRY_SURCHARGE_PER_TON)
