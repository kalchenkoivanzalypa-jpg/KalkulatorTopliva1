#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Утилиты для расчета расстояний и стоимости доставки
"""

import logging
import math
import os
import difflib
from typing import Optional, Tuple
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from db.database import CityDestination

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AUTO_RATE_PER_TKM = float(os.getenv("AUTO_RATE_PER_TKM", "9.0"))
SAKHALIN_CITY_KEYS = {
    "южно сахалинск",
    "корсаков",
    "холмск",
    "оха",
    "поронайск",
    "долинск",
    "невельск",
    "анива",
    "углегорск",
    "томари",
    "макаров",
    "ноглики",
}
SAKHALIN_CITY_COORDS: dict[str, tuple[float, float]] = {
    "южно сахалинск": (46.9592, 142.7381),
    "корсаков": (46.6324, 142.7885),
    "холмск": (47.0478, 142.0569),
    "оха": (53.5738, 142.9478),
    "поронайск": (49.2218, 143.1001),
    "долинск": (47.3259, 142.7951),
    "невельск": (46.6527, 141.8615),
    "анива": (46.7156, 142.5277),
    "углегорск": (49.0815, 142.0323),
    "томари": (47.7622, 142.0617),
    "макаров": (48.6250, 142.7789),
    "ноглики": (51.7903, 143.1294),
}

# Крупные пункты, для которых в CityDestination нередко попадают неверные координаты
# (одноимённые деревни, «в области» и т.п.) — фиксируем центр города для расчёта доставки.
CANONICAL_CITY_COORDS: dict[str, tuple[float, float]] = {
    # Город Свободный (Амурская обл.), не одноимённые пункты в других регионах
    "свободный": (51.3807, 128.1285),
    "тында": (55.1547, 124.7238),
    # Ванино (Хабаровский край): иначе часто подхватывается одноимённое в других регионах/с неверными координатами.
    "ванино": (49.0850, 140.2650),
    # Якутск (центр); ж/д тариф считается до Нижнего Бестяха — см. rail_logistics.CITY_KEY_TO_RAIL_DEST_ESR
    "якутск": (62.0278, 129.7325),
    "архангельск": (64.5399, 40.5158),
    "мурманск": (68.9585, 33.0827),
    "петрозаводск": (61.7850, 34.3469),
    "северодвинск": (64.5625, 39.8182),
    "вологда": (59.2181, 39.8978),
    "сыктывкар": (61.6688, 50.8365),
    # Благовещенск (Амурская обл.) — иначе часто подхватывается одноимённый город под Уфой.
    "благовещенск": (50.2906, 127.5272),
    # Артём (Приморский край) — иначе может подтянуться одноимённый населённый пункт в другом регионе.
    "артем": (43.3595, 132.1858),
}


def _strip_trailing_region_suffix(s: str) -> str:
    """
    Убирает хвост «… амурская область», «… хабаровский край» и т.п., чтобы
    «город Свободный Амурская область» сходился с записью «Свободный» в БД и CANONICAL.
    """
    t = (s or "").strip()
    if not t:
        return t
    # Длинные фразы — первыми (жадное совпадение по началу строки не нужно)
    suffixes = (
        "еврейская автономная область",
        "чукотский автономный округ",
        "ханты мансийский автономный округ югра",
        "ямало ненецкий автономный округ",
        "республика саха якутия",
        "амурская область",
        "амурская обл",
        "магаданская область",
        "сахалинская область",
        "приморский край",
        "хабаровский край",
        "забайкальский край",
        "камчатский край",
        "пермский край",
        "красноярский край",
        "ставропольский край",
        "алтайский край",
    )
    low = t.lower()
    for suf in suffixes:
        suf_l = suf.lower()
        if low.endswith(suf_l):
            cut = len(t) - len(suf_l)
            t = t[:cut].rstrip(" ,-—–").strip()
            break
    return t


def normalize_city_name_key(city_name: str) -> str:
    """
    Нормализация имени для поиска в БД:
    - lowercase
    - ё->е
    - удаляем префиксы типа "г.", "с.", "п.", "ст." и слова "город/село/поселок"
    - схлопываем пробелы и чистим края
    """
    import re

    s = (city_name or "").strip().lower()
    s = s.replace("ё", "е")
    s = s.replace("—", "-").replace("–", "-").replace("−", "-").replace("‑", "-")
    # Если в имени есть регион в скобках (например "Псков (Псковская область)"),
    # убираем его для устойчивого поиска по "Псков".
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s).strip()
    # Для устойчивого поиска считаем дефис и пробел эквивалентными
    s = s.replace("-", " ")

    # Ж/д префиксы — иначе «ст. Тында» не совпадает с CANONICAL «тында» и с названием в БД
    for prefix in (
        "ж/д станция ",
        "жд станция ",
        "станция ",
        "ст.",
        "ст ",
        "ж/д ",
        "жд ",
    ):
        if s.startswith(prefix):
            s = s[len(prefix) :].strip()
            break

    s = re.sub(r"^(г\.|город|гор)\s+", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^(с\.|село)\s+", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^(п\.|поселок|посел|пос)\s+", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+г\.?$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^[,(]+|[),.]+$", "", s)
    s = _strip_trailing_region_suffix(s)
    s = re.sub(r"\s+", " ", s)
    return s


async def get_coordinates_from_city(city_name: str, session: AsyncSession = None) -> Optional[Tuple[float, float]]:
    """
    Получение координат города по названию.
    Сначала проверяет кэш в БД, затем обращается к API.
    """
    # Очищаем название
    city_name = (city_name or "").strip()
    city_name_key = normalize_city_name_key(city_name)
    city_name_ye = city_name.replace("ё", "е").replace("Ё", "Е").strip()
    is_sakhalin_query = city_name_key in SAKHALIN_CITY_KEYS
    if is_sakhalin_query and city_name_key in SAKHALIN_CITY_COORDS:
        # Для ключевых городов Сахалина используем эталонные координаты,
        # чтобы избежать ложных совпадений с одноименными пунктами на материке.
        return SAKHALIN_CITY_COORDS[city_name_key]

    if city_name_key in CANONICAL_CITY_COORDS:
        logger.info("✅ Эталонные координаты для населённого пункта: %s", city_name_key)
        return CANONICAL_CITY_COORDS[city_name_key]

    def _is_sakhalin_coord(lat: float, lon: float) -> bool:
        return 45.0 <= float(lat) <= 55.0 and 141.0 <= float(lon) <= 146.0
    
    logger.info(f"🔍 Поиск координат для: {city_name}")
    
    # Сначала проверяем в локальном кэше
    if session:
        # 0.5) Быстрый путь для нашей схемы импорта: name может быть вида "Псков (Псковская область)".
        # В SQLite lower()/LIKE плохо работают с кириллицей, поэтому делаем case-sensitive LIKE по исходной строке.
        # Чтобы не зависеть от регистра ввода ("тында" vs "Тында"), пробуем несколько вариантов написания.
        def _title_first(s: str) -> str:
            s = (s or "").strip()
            if not s:
                return s
            return s[0].upper() + s[1:]

        probes: list[str] = []
        for s in [city_name, city_name_ye]:
            s0 = (s or "").strip()
            if not s0:
                continue
            probes.append(s0)
            probes.append(_title_first(s0))

        # 0.6) Сначала устойчивый путь: регистр не должен менять координаты.
        # Быстрый путь ниже (== / case-sensitive LIKE) может взять «первую попавшуюся» строку
        # с тем же префиксом/написанием, если не проверять normalize(name)==city_name_key.
        if city_name_key:
            result = await session.execute(
                select(CityDestination)
                .where(
                    func.replace(func.lower(func.trim(CityDestination.name)), "ё", "е").ilike(
                        f"%{city_name_key}%"
                    )
                )
                .order_by(CityDestination.request_count.desc())
                .limit(200)
            )
            pool = result.scalars().all()
            if pool:
                best = None
                best_pop = -1
                for c in pool:
                    if not c or not c.name:
                        continue
                    if normalize_city_name_key(c.name) != city_name_key:
                        continue
                    pop = int(getattr(c, "request_count", 0) or 0)
                    if pop > best_pop:
                        best_pop = pop
                        best = c
                if best is not None:
                    logger.info(f"✅ Найдено по нормализованному ключу (устойчиво к регистру): {best.name}")
                    return (best.latitude, best.longitude)

        seen_probe: set[str] = set()
        for probe in probes:
            if not probe or probe in seen_probe:
                continue
            seen_probe.add(probe)
            result = await session.execute(
                select(CityDestination)
                .where(
                    (CityDestination.name == probe)
                    | (CityDestination.name.like(f"{probe} (%)"))
                )
                .order_by(CityDestination.request_count.desc())
                .limit(25)
            )
            for row in result.scalars().all():
                if row and normalize_city_name_key(row.name) == city_name_key:
                    return (row.latitude, row.longitude)

        # 0) Для сахалинских городов сначала пытаемся взять запись из Сахалинской области.
        if is_sakhalin_query:
            result = await session.execute(
                select(CityDestination)
                .where(
                    func.replace(func.lower(func.trim(CityDestination.region)), "ё", "е").ilike("%сахалин%"),
                    func.replace(func.lower(func.trim(CityDestination.name)), "ё", "е").ilike(f"%{city_name_key}%"),
                )
                .order_by(CityDestination.request_count.desc())
                .limit(100)
            )
            sak_rows = result.scalars().all()
            for c in sak_rows:
                if normalize_city_name_key(c.name) == city_name_key:
                    logger.info(f"✅ Найдено в Сахалинской области: {c.name}")
                    return (c.latitude, c.longitude)
            if sak_rows:
                c0 = sak_rows[0]
                logger.info(f"✅ Найдено в Сахалинской области (по вхождению): {c0.name}")
                return (c0.latitude, c0.longitude)

        # 1) Точное совпадение по ё->е (короткий путь)
        result = await session.execute(
            select(CityDestination).where(CityDestination.name == city_name_ye).limit(1)
        )
        city = result.scalar_one_or_none()
        if city:
            if is_sakhalin_query:
                reg = str(getattr(city, "region", "") or "").lower().replace("ё", "е")
                if ("сахалин" not in reg) and not _is_sakhalin_coord(city.latitude, city.longitude):
                    city = None
            if city is None:
                pass
            else:
                return (city.latitude, city.longitude)

        # 2) Подстрочное совпадение с ё->е + нормализованным ключом.
        # Берём несколько кандидатов и выбираем самый "каноничный" через Python-логику.
        result = await session.execute(
            select(CityDestination)
            .where(
                func.replace(func.lower(func.trim(CityDestination.name)), "ё", "е").ilike(
                    f"%{city_name_key}%"
                )
            )
            .order_by(CityDestination.request_count.desc())
            .limit(25)
        )
        candidates = result.scalars().all()
        if candidates:
            if is_sakhalin_query:
                sakhalin_candidates = [
                    c
                    for c in candidates
                    if "сахалин" in str(getattr(c, "region", "") or "").lower().replace("ё", "е")
                ]
                if sakhalin_candidates:
                    candidates = sakhalin_candidates
            for c in candidates:
                if normalize_city_name_key(c.name) == city_name_key:
                    logger.info(f"✅ Найдено по нормализованному совпадению: {c.name}")
                    return (c.latitude, c.longitude)
            # Если точного нормализованного не нашли — НЕ берём первый попавшийся,
            # иначе короткие запросы типа "ангарск" часто «уезжают» в "нижнеангарск".
            # Важно: не перезаписывать одинаковые ключи — первые строки уже отсортированы
            # по request_count DESC (т.е. более популярные города должны выигрывать).
            key_to_city = {}
            for c in candidates:
                if not c or not c.name:
                    continue
                k = normalize_city_name_key(c.name)
                if k and k not in key_to_city:
                    key_to_city[k] = c
            matches = difflib.get_close_matches(city_name_key, list(key_to_city.keys()), n=1, cutoff=0.90)
            if matches:
                picked = key_to_city[matches[0]]
                logger.info(f"✅ Найдено по похожему названию среди кандидатов: {picked.name}")
                return (picked.latitude, picked.longitude)
            logger.warning("⚠️ Подстрочное совпадение неоднозначно (%s) — возвращаем None, пусть сработает поиск по станции.", city_name_key)
            return None

        # 3) Мягкий фоллбек для опечаток: ищем похожее название среди кандидатов по префиксу.
        # Пример: "Комсамольск-на-Амуре" -> "Комсомольск-на-Амуре".
        prefix = city_name_key[:3]
        if prefix:
            result = await session.execute(
                select(CityDestination)
                .where(
                    func.replace(func.lower(func.trim(CityDestination.name)), "ё", "е").ilike(
                        f"{prefix}%"
                    )
                )
                .order_by(CityDestination.request_count.desc())
                .limit(300)
            )
            fuzzy_pool = result.scalars().all()
            if fuzzy_pool:
                key_to_city = {}
                for c in fuzzy_pool:
                    if not c or not c.name:
                        continue
                    k = normalize_city_name_key(c.name)
                    if k and k not in key_to_city:
                        key_to_city[k] = c
                matches = difflib.get_close_matches(
                    city_name_key,
                    list(key_to_city.keys()),
                    n=1,
                    cutoff=0.84,
                )
                if matches:
                    picked = key_to_city[matches[0]]
                    logger.info(f"✅ Найдено по похожему названию: {picked.name}")
                    return (picked.latitude, picked.longitude)
    
    # Если в БД нет, обращаемся к API (заглушка)
    logger.warning(f"❌ Город '{city_name}' не найден в БД, возвращаем None")
    return None


async def get_best_transport_type(distance_km: float) -> str:
    """
    Определение оптимального типа транспорта по расстоянию
    """
    if distance_km < 500:
        return 'auto'  # для коротких расстояний авто эффективнее
    elif distance_km > 1500:
        return 'rail'  # для дальних расстояний Ж/Д эффективнее
    else:
        # Для средних расстояний (500-1500 км) нужно сравнивать стоимость
        # Пока возвращаем оба варианта, но в боте будет выбор
        return 'auto'  # по умолчанию авто


def canonical_fuel_display_name(product_name: str) -> str:
    """
    Каноническое отображение топлива:
    - АИ-92/95/100-К5
    - ДТ-А/Е/З/Л-К5 (3 => З)
    - ТС-1
    - Мазут топочный М100
    """
    import re

    s = (product_name or "").strip()
    if not s:
        return s

    s = (
        s.replace("—", "-")
        .replace("–", "-")
        .replace("−", "-")
        .replace("‑", "-")
    )

    # ТС-1
    if re.search(r"\bТС\s*-?\s*1\b|\bTC\s*-?\s*1\b", s, flags=re.IGNORECASE):
        return "ТС-1"

    # Мазут
    if "мазут" in s.lower():
        # По ТЗ всегда показываем М100
        return "Мазут топочный М100"

    # АИ
    m = re.search(r"(АИ)\s*-\s*(\d+)\s*-\s*К5", s, flags=re.IGNORECASE)
    if m:
        val = m.group(2)
        return f"АИ-{val}-К5"

    # ДТ
    m = re.search(r"ДТ\s*-\s*([A-ZА-ЯЁ0-9])\s*-\s*К5", s, flags=re.IGNORECASE)
    if m:
        mid = m.group(1).upper()
        if mid == "3":
            mid = "З"
        if mid == "0":
            mid = "О"
        return f"ДТ-{mid}-К5"

    return s


async def get_delivery_rate(distance_km: float, transport_type: str, session=None) -> float:
    """
    Получение ставки доставки в руб/т·км
    
    Для авто - упрощенные ставки
    Для Ж/Д - калиброванные ставки по данным РЖД
    """
    if transport_type == 'auto':
        # По бизнес-референсу: единая ставка авто в руб/(т*км)
        return AUTO_RATE_PER_TKM
    else:
        # Ж/Д тарифы - калиброваны по данным с сайта РЖД
        # Короткие расстояния (до 500 км) - очень дорого
        if distance_km <= 500:
            return 7.41
        
        # 500 - 1000 км
        elif distance_km <= 1000:
            return 7.41 - (distance_km - 500) * (1.21 / 500)
        
        # 1000 - 2000 км
        elif distance_km <= 2000:
            return 6.20 - (distance_km - 1000) * (1.20 / 1000)
        
        # 2000 - 3000 км
        elif distance_km <= 3000:
            return 5.00 - (distance_km - 2000) * (0.70 / 1000)
        
        # 3000 - 4000 км
        elif distance_km <= 4000:
            return 4.30 - (distance_km - 3000) * (0.40 / 1000)
        
        # 4000 - 4649 км (до Новой Еловки)
        elif distance_km <= 4649:
            return 3.90 - (distance_km - 4000) * (0.33 / 649)
        
        # 4649 - 5000 км
        elif distance_km <= 5000:
            return 3.57 + (distance_km - 4649) * (0.03 / 351)
        
        # 5000 - 6000 км
        elif distance_km <= 6000:
            return 3.60
        
        # 6000 - 7101 км (до Уфы)
        elif distance_km <= 7101:
            return 3.60 - (distance_km - 6000) * (0.08 / 1101)
        
        # 7101 - 7522 км (до Пурпе)
        elif distance_km <= 7522:
            return 3.52 - (distance_km - 7101) * (0.03 / 421)
        
        # Дальше 7522 км
        else:
            return 3.49 - (distance_km - 7522) * (0.01 / 500)


def get_delivery_rate_sync(distance_km: float, transport_type: str = 'rail') -> float:
    """
    Синхронная версия get_delivery_rate для использования в не-async функциях
    """
    if transport_type == 'auto':
        return AUTO_RATE_PER_TKM
    else:
        if distance_km <= 500:
            return 7.41
        elif distance_km <= 1000:
            return 7.41 - (distance_km - 500) * (1.21 / 500)
        elif distance_km <= 2000:
            return 6.20 - (distance_km - 1000) * (1.20 / 1000)
        elif distance_km <= 3000:
            return 5.00 - (distance_km - 2000) * (0.70 / 1000)
        elif distance_km <= 4000:
            return 4.30 - (distance_km - 3000) * (0.40 / 1000)
        elif distance_km <= 4649:
            return 3.90 - (distance_km - 4000) * (0.33 / 649)
        elif distance_km <= 5000:
            return 3.57 + (distance_km - 4649) * (0.03 / 351)
        elif distance_km <= 6000:
            return 3.60
        elif distance_km <= 7101:
            return 3.60 - (distance_km - 6000) * (0.08 / 1101)
        elif distance_km <= 7522:
            return 3.52 - (distance_km - 7101) * (0.03 / 421)
        else:
            return 3.49 - (distance_km - 7522) * (0.01 / 500)


def calculate_delivery_cost(distance_km: float, volume_tonns: float, transport_type: str = 'rail') -> dict:
    """
    Расчет стоимости доставки (упрощенный)
    """
    rate = get_delivery_rate_sync(distance_km, transport_type)
    total_cost = distance_km * volume_tonns * rate
    
    return {
        'distance_km': round(distance_km, 1),
        'volume_tonns': volume_tonns,
        'rate_per_ton_km': round(rate, 3),
        'total_cost': round(total_cost, 2),
        'cost_per_ton': round(total_cost / volume_tonns, 2)
    }


# Тестирование функций
if __name__ == "__main__":
    print("=" * 60)
    print("ТЕСТИРОВАНИЕ СТАВОК ДОСТАВКИ")
    print("=" * 60)
    
    test_distances = [100, 398, 500, 1000, 2000, 3000, 4000, 4649, 5000, 6000, 7101, 7522, 8000]
    
    print(f"\n{'Расст':>6} | {'Авто':>8} | {'Ж/Д':>8} | {'Комментарий':<30}")
    print("-" * 60)
    
    for dist in test_distances:
        auto_rate = get_delivery_rate_sync(dist, 'auto')
        rail_rate = get_delivery_rate_sync(dist, 'rail')
        
        comment = ""
        if dist == 398:
            comment = "ст. Дземги"
        elif dist == 4649:
            comment = "ст. Новая Еловка"
        elif dist == 7101:
            comment = "Уфа-группа"
        elif dist == 7522:
            comment = "ст. Пурпе"
        
        print(f"{dist:6d} | {auto_rate:8.3f} | {rail_rate:8.3f} | {comment}")