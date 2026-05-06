# -*- coding: utf-8 -*-
"""
Логистика Ж/Д: ближайшая станция к точке назначения, координаты «отправления» у базиса.
"""
from __future__ import annotations

import logging
import os
import re
import unicodedata
from typing import Optional, Tuple

from haversine import Unit, haversine
from sqlalchemy import cast, or_, select, String

logger = logging.getLogger(__name__)
SAKHALIN_FERRY_SURCHARGE_PER_TON = float(os.getenv("SAKHALIN_FERRY_SURCHARGE_PER_TON", "4239"))

# Временные точечные фиксы логистики (override доставки, ₽/т) для конкретных направлений.
# Нужны, чтобы «прибить» стоимость Ж/Д логистики от определённого базиса до ряда городов.
_BASIS_KEY_ANGARSK_GROUP = "ангарск группа станций"

# Фиксированные значения доставки, ₽/т. Применяются для всех товаров, если базис = «Ангарск-группа станций».
# Источник: «Ставка за тонну» (₽/т) со скринов предварительной стоимости перевозки.
_ANGARSK_FIXED_DELIVERY_PER_TON: dict[str, float] = {
    "хабаровск 1": 12311.79,
    "находка": 15277.03,
    "владивосток": 14955.02,
    "артем": 14955.02,
    "южно сахалинск": 17414.67,
    "благовещенск": 10819.99,
    "тында": 9398.79,
    "нижний бестях": 12311.79,
}

# Населённые пункты без станции в справочнике / без нужного ЕСР: тариф ТР №4 до указанного узла
# (например, Якутск — груз доходит по ж/д до Нижнего Бестяха, дальше переправа через Лену).
CITY_KEY_TO_RAIL_DEST_ESR: dict[str, str] = {
    "якутск": "913403",
    # Благовещенск (Амурская область): чтобы не путать с Благовещенском рядом с Уфой
    "благовещенск": "954704",
    # Свободный (Амурская область): ж/д узел «Михайло-Чесноковская»
    "свободный": "953701",
    # Ванино (Хабаровский край): порт Ванино
    "ванино": "967600",
    # Артём (Приморский край): «Артем-Приморский I»
    "артем": "982403",
}

# Точечные исправления координат для известных проблемных станций.
# Нужны, когда в rail_stations попали неверные lat/lon (например, из старого импорта/ручных правок).
_ESR_COORD_OVERRIDES: dict[str, tuple[float, float]] = {
    # Ванино (Хабаровский край)
    "967600": (49.0850, 140.2650),
    "967704": (49.0850, 140.2650),
    "967808": (49.0850, 140.2650),
    # Свободный / Михайло-Чесноковская (Амурская область) — берём координаты города как надёжный якорь.
    "953701": (51.3807, 128.1285),
}

# Отдельно стоящие 5–6 цифр в строке ввода — код ЕСР станции (Книга 2 / справочник).
# Встречается 5-значная запись без ведущего нуля (Excel/ручной ввод).
_ESR_5_6_DIGITS_RE = re.compile(r"(?<!\d)(\d{5,6})(?!\d)")


def _ascii_digits_only(s: str) -> str:
    """Только цифры 0–9: PDF/Word часто дают unicode-цифры и неразрывные пробелы."""
    out: list[str] = []
    for ch in s or "":
        if ch in "0123456789":
            out.append(ch)
            continue
        try:
            d = unicodedata.digit(ch)
        except (TypeError, ValueError):
            continue
        if 0 <= d <= 9:
            out.append(str(d))
    return "".join(out)


def extract_first_esr_code(destination_raw: str, destination_name_key: str) -> Optional[str]:
    """Первый код ЕСР из ввода (5–6 цифр) → канон 6 цифр (zfill)."""
    for part in (destination_raw or "", destination_name_key or ""):
        t = _ascii_digits_only(part)
        m = _ESR_5_6_DIGITS_RE.search(t)
        if m:
            code = m.group(1)
            if code.isdigit() and len(code) in (5, 6):
                return code.zfill(6)
    return None


def normalize_esr_to_6(val: object) -> Optional[str]:
    """
    Нормализация ЕСР к 6 цифрам:
    - принимает '98765' -> '098765'
    - принимает '932207.0' -> '932207'
    - отбрасывает мусор/слишком длинные значения.
    """
    if val is None:
        return None
    s = str(val).strip().replace(" ", "")
    if s.endswith(".0") and len(s) > 2:
        head = s[:-2]
        if head.isdigit():
            s = head
    if not s.isdigit() or len(s) > 6 or len(s) < 5:
        return None
    return s.zfill(6)


def coords_from_rail_station(st) -> Optional[Tuple[float, float]]:
    """Координаты станции для геопоиска или None, если в БД нет lat/lon."""
    if st is None:
        return None
    esr = normalize_esr_to_6(getattr(st, "esr_code", None))
    if esr and esr in _ESR_COORD_OVERRIDES:
        return _ESR_COORD_OVERRIDES[esr]
    lat, lon = getattr(st, "latitude", None), getattr(st, "longitude", None)
    if lat is None or lon is None:
        return None
    try:
        return float(lat), float(lon)
    except (TypeError, ValueError):
        return None


def geo_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Расстояние по дуге большого круга, км."""
    return float(haversine((lat1, lon1), (lat2, lon2), unit=Unit.KILOMETERS))


async def rail_dest_station_for_city_key(session, destination_name_key: str):
    """
    Если для нормализованного ключа города задан конечный ж/д узел — вернуть эту станцию (для ЕСР в ТР №4).
    """
    from db.database import RailStation

    key = (destination_name_key or "").strip().lower().replace("ё", "е")
    if not key:
        return None
    esr = CITY_KEY_TO_RAIL_DEST_ESR.get(key)
    if not esr:
        return None
    st = await find_rail_station_by_esr_code(session, esr)
    if st is None:
        logger.warning(
            "Ключ «%s» привязан к ЕСР %s, но станция не найдена в rail_stations",
            key,
            esr,
        )
    else:
        logger.info(
            "🚉 Для «%s» ж/д тариф до узла %s (ЕСР %s)",
            key,
            st.name,
            esr,
        )
    return st


def _normalize_esr_cell_value(val: object) -> str:
    """Привести значение esr_code из БД к виду «только 6 цифр» где возможно."""
    if val is None:
        return ""
    s = str(val).strip().replace(" ", "")
    # «910000.0» из Excel/CSV
    if s.endswith(".0") and len(s) > 2:
        head = s[:-2]
        if head.isdigit():
            return head
    return s


def _esr_canonical_6_from_cell(val: object) -> Optional[str]:
    """
    ЕСР для сравнения: до 6 десятичных цифр с ведущими нулями (как в Книге 2).
    В БД часто «98765» вместо «098765» после Excel.
    """
    s = _normalize_esr_cell_value(val)
    if not s or not s.isdigit():
        return None
    if len(s) > 6:
        return None
    return s.zfill(6)


async def find_rail_station_by_esr_code(session, esr: str):
    """
    Одна станция по ЕСР (сначала активные, при отсутствии — неактивная с тем же кодом).
    Сравнение с нормализацией ведущих нулей; при необходимости — полный перебор id+esr_code.
    """
    from db.database import RailStation

    esr = (esr or "").strip()
    if not esr or not re.fullmatch(r"\d{6}", esr):
        return None

    ec_text = cast(RailStation.esr_code, String)

    def _pick(hits: list) -> Optional[object]:
        if not hits:
            return None
        canon = [st for st in hits if _esr_canonical_6_from_cell(getattr(st, "esr_code", None)) == esr]
        if not canon:
            return None
        if len(canon) > 1:
            logger.warning("ESR %s: в БД несколько строк, берём первую", esr)
        return canon[0]

    # 1) Префикс как в справочнике «098765…»
    result = await session.execute(
        select(RailStation)
        .where(RailStation.is_active.is_(True))
        .where(ec_text.like(esr + "%"))
        .limit(80)
    )
    picked = _pick(list(result.scalars().all()))
    if picked is not None:
        return picked

    # 2) Точное совпадение сырого поля (Excel / PG)
    result2 = await session.execute(
        select(RailStation)
        .where(RailStation.is_active.is_(True))
        .where(
            or_(
                RailStation.esr_code == esr,
                RailStation.esr_code == esr + ".0",
            )
        )
        .limit(20)
    )
    picked = _pick(list(result2.scalars().all()))
    if picked is not None:
        return picked

    # 3) Без ведущих нулей в БД: LIKE «98765%» только если суффикс достаточно длинный (меньше ложных срабатываний)
    stripped = esr.lstrip("0") or "0"
    if stripped != esr and len(stripped) >= 5:
        result3 = await session.execute(
            select(RailStation)
            .where(RailStation.is_active.is_(True))
            .where(ec_text.like(stripped + "%"))
            .limit(120)
        )
        picked = _pick(list(result3.scalars().all()))
        if picked is not None:
            return picked

    # 4) Фоллбек: все активные с непустым esr_code (легче по памяти, чем полные ORM-объекты)
    result4 = await session.execute(
        select(RailStation.id, RailStation.esr_code).where(RailStation.is_active.is_(True)).where(RailStation.esr_code.isnot(None))
    )
    for rid, ec in result4.all():
        if _esr_canonical_6_from_cell(ec) == esr:
            st = await session.get(RailStation, rid)
            if st is not None:
                return st

    # 5) Неактивная запись с тем же ЕСР (лучше тариф, чем «нет координат»)
    result5 = await session.execute(
        select(RailStation.id, RailStation.esr_code).where(RailStation.is_active.is_(False)).where(RailStation.esr_code.isnot(None))
    )
    for rid, ec in result5.all():
        if _esr_canonical_6_from_cell(ec) == esr:
            st = await session.get(RailStation, rid)
            if st is not None:
                logger.warning("ЕСР %s: найдена только неактивная станция id=%s", esr, rid)
                return st

    logger.warning(
        "ЕСР %s нет в rail_stations (или формат кода в БД не совпадает). Проверьте импорт Книги 2 / upsert_critical_rail_stations.",
        esr,
    )
    return None


async def resolved_rail_dest_station_for_destination(
    session, destination_raw: str, destination_name_key: str
):
    """
    Станция для ж/д тарифа: сначала явная привязка города к узлу (см. CITY_KEY_TO_RAIL_DEST_ESR),
    затем поиск по справочнику (как в find_nearest_basises).
    """
    st = await rail_dest_station_for_city_key(session, destination_name_key or "")
    if st is not None:
        return st
    return await find_rail_station_for_destination(
        session, destination_raw or "", destination_name_key or ""
    )


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
    0) шестизначный ЕСР в строке (910000, «ЕСР 953627», «ст. 913403»)
    1) точное совпадение по settlement_name
    2) точное совпадение по station.name
    3) нестрогое совпадение (подстрока) по station.name/settlement_name
    """
    from db.database import RailStation
    from utils import normalize_city_name_key

    esr_hint = extract_first_esr_code(destination_raw or "", destination_name_key or "")
    if esr_hint:
        st_esr = await find_rail_station_by_esr_code(session, esr_hint)
        if st_esr is not None:
            logger.info("🚉 Станция по ЕСР %s: %s", esr_hint, st_esr.name)
            return st_esr

    # 0.5) Явная привязка «город → конечный ж/д узел» (например Якутск→Нижний Бестях,
    # Благовещенск (Амур) → Благовещенск ЕСР 954704). Это защищает от однофамильцев
    # в справочнике при поиске "по названию".
    try:
        city_key = (destination_name_key or "").strip().lower().replace("ё", "е")
        mapped_esr = CITY_KEY_TO_RAIL_DEST_ESR.get(city_key)
        if mapped_esr:
            st_mapped = await find_rail_station_by_esr_code(session, mapped_esr)
            if st_mapped is not None:
                logger.info("🚉 Город «%s» привязан к ЕСР %s: %s", city_key, mapped_esr, st_mapped.name)
                return st_mapped
    except Exception:
        pass

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
    settlement_exact = [
        st
        for st in stations
        if st.settlement_name and normalize_city_name_key(st.settlement_name) == key
    ]
    if settlement_exact:
        # если несколько однофамильцев — предпочитаем сахалинскую станцию (ESR 99xxxx / bbox / регион)
        for st in settlement_exact:
            if is_sakhalin_station(st):
                return st
        return settlement_exact[0]

    # 2) Точное совпадение по названию станции
    name_exact = [st for st in stations if _normalized_station_key(st.name or "") == key]
    if name_exact:
        for st in name_exact:
            if is_sakhalin_station(st):
                return st
        return name_exact[0]

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


def _normalize_basis_name_key(name: str) -> str:
    s = (name or "").strip().lower().replace("ё", "е")
    s = s.replace("—", "-").replace("–", "-").replace("−", "-").replace("‑", "-")
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def fixed_delivery_per_ton_override(basis_name: str, destination_name_key: str) -> Optional[float]:
    """
    Фикс доставки (₽/т) для частных случаев.
    Возвращает значение доставки (₽/т) или None (если override не применяется).
    """
    try:
        bkey = _normalize_basis_name_key(basis_name)
        dkey = (destination_name_key or "").strip().lower().replace("ё", "е")
    except Exception:
        return None
    # Базис: «Ангарск-группа станций»
    if bkey != _BASIS_KEY_ANGARSK_GROUP:
        return None
    return _ANGARSK_FIXED_DELIVERY_PER_TON.get(dkey)
