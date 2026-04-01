from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

from book2_parser import StationTransit, parse_book2_part1, parse_book2_part1_with_display, normalize_base_name
from book3_parser import parse_book3_tp_distances


@dataclass(frozen=True)
class Tariff4Data:
    # Книга 2: станция -> список (tp, dist_km)
    book2_station_to_transits: Dict[str, List[StationTransit]]
    # Для UX: base_station_name -> display_name
    book2_station_base_to_name: Dict[str, str]
    # Книга 3: tp -> tp -> dist_km
    book3_tp_to_tp: Dict[str, Dict[str, int]]


@lru_cache(maxsize=4)
def load_tariff4_data(data_root: str = "data") -> Tariff4Data:
    book2_folder = f"{data_root}/kniga2"
    book3_folder = f"{data_root}/kniga3"

    book2_station_to_transits, book2_station_base_to_name = parse_book2_part1_with_display(book2_folder)
    book3_tp_to_tp = parse_book3_tp_distances(book3_folder)
    return Tariff4Data(
        book2_station_to_transits=book2_station_to_transits,
        book2_station_base_to_name=book2_station_base_to_name,
        book3_tp_to_tp=book3_tp_to_tp,
    )


def resolve_station_base_name(station_query: str, book2_station_to_transits: Dict[str, List[StationTransit]]) -> Optional[str]:
    """
    Простейшее разрешение: если точное совпадение не найдено, пробуем `contains`
    по нормализованному имени.
    """
    q = normalize_base_name(station_query)
    if q in book2_station_to_transits:
        return q

    # грубый подбор: q входит в ключ или ключ входит в q
    best_key = None
    best_score = None
    for key in book2_station_to_transits.keys():
        if q in key or key in q:
            # ближе = ключ короче (эвристика)
            score = (len(key), len(q))
            if best_score is None or score < best_score:
                best_score = score
                best_key = key
    return best_key


def logistic_distance_km(from_station: str, to_station: str, data_root: str = "data") -> int:
    """
    Базовая схема (по смыслу вводных Книг 2 и 3):

    Для каждого TP-пункта:
      dist(A, B) = min_{tp1 in TPs(A), tp2 in TPs(B)} ( d(A,tp1) + d(tp1,tp2) + d(tp2,B) )

    Где:
      d(A,tp) берём из Книги 2 (станция -> ближайшие транзитные пункты),
      d(tp1,tp2) берём из Книги 3 (таблица тарифных расстояний между ТП).
    """
    data = load_tariff4_data(data_root=data_root)

    from_base = resolve_station_base_name(from_station, data.book2_station_to_transits)
    to_base = resolve_station_base_name(to_station, data.book2_station_to_transits)
    if not from_base:
        raise ValueError(f"Не нашёл станцию/пункт отправления: {from_station!r}")
    if not to_base:
        raise ValueError(f"Не нашёл станцию/пункт назначения: {to_station!r}")

    from_transits = data.book2_station_to_transits[from_base]
    to_transits = data.book2_station_to_transits[to_base]
    if not from_transits:
        raise ValueError(f"Для станции {from_station!r} нет данных Книги 2 (нет ТП)")
    if not to_transits:
        raise ValueError(f"Для станции {to_station!r} нет данных Книги 2 (нет ТП)")

    best = None
    for t1 in from_transits:
        for t2 in to_transits:
            tp1 = t1.tp_base_name
            tp2 = t2.tp_base_name

            # Книга 3 часто не содержит явных нулевых расстояний tp->тот же tp.
            # Для таких случаев считаем mid=0, иначе логика "склеивания" даёт завышенные
            # маршруты (например 636+630=1266 вместо локальной связки через Иркутск).
            if tp1 == tp2:
                mid = 0
            else:
                mid = data.book3_tp_to_tp.get(tp1, {}).get(tp2)
            if mid is None:
                # Если строгое сопоставление не совпало по базовому имени — пропускаем.
                continue

            total = t1.distance_km + mid + t2.distance_km
            if best is None or total < best:
                best = total

    if best is None:
        raise ValueError("Не удалось посчитать: нет совпадений ТП в Книгах 2 и 3")

    return best


def logistic_distance_verbose(from_station: str, to_station: str, data_root: str = "data") -> Tuple[str, str, int]:
    """
    Возвращает (from_station_display, to_station_display, distance_km) для UX.
    """
    data = load_tariff4_data(data_root=data_root)

    from_base = resolve_station_base_name(from_station, data.book2_station_to_transits)
    to_base = resolve_station_base_name(to_station, data.book2_station_to_transits)
    if not from_base or not to_base:
        raise ValueError("Не нашлось station/punkt в Книге 2.")

    from_display = data.book2_station_base_to_name.get(from_base, from_station.strip())
    to_display = data.book2_station_base_to_name.get(to_base, to_station.strip())

    from_transits = data.book2_station_to_transits[from_base]
    to_transits = data.book2_station_to_transits[to_base]
    best = None

    for t1 in from_transits:
        for t2 in to_transits:
            tp1 = t1.tp_base_name
            tp2 = t2.tp_base_name
            if tp1 == tp2:
                mid = 0
            else:
                mid = data.book3_tp_to_tp.get(tp1, {}).get(tp2)
            if mid is None:
                continue
            total = t1.distance_km + mid + t2.distance_km
            if best is None or total < best:
                best = total

    if best is None:
        raise ValueError("Не удалось посчитать: нет совпадений ТП в Книгах 2 и 3")

    return from_display, to_display, int(best)

