from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Optional, Tuple

from book1_parser_graph import build_book1_weighted_graph, resolve_city_to_station_code
from dijkstra import shortest_path
from logistics import logistic_distance_verbose


@dataclass(frozen=True)
class DistanceResult:
    from_station: str
    to_station: str
    distance_km: int
    method: str  # "book1" | "book2+3" | "min(book1,book2+3)"


@lru_cache(maxsize=1)
def _load_book1(data_root: str = "data"):
    graph, code_to_name = build_book1_weighted_graph(f"{data_root}/kniga1")
    return graph, code_to_name


def _try_calc_book1(from_city: str, to_city: str, data_root: str) -> Optional[Tuple[str, str, int]]:
    graph, code_to_name = _load_book1(data_root=data_root)
    try:
        from_code = resolve_city_to_station_code(from_city, code_to_name)
        to_code = resolve_city_to_station_code(to_city, code_to_name)
    except Exception:
        return None

    distance, _path = shortest_path(graph, from_code, to_code)
    if distance is None:
        return None

    return code_to_name[from_code], code_to_name[to_code], int(distance)


def _try_calc_book2_book3(from_station_query: str, to_station_query: str, data_root: str) -> Optional[Tuple[str, str, int]]:
    try:
        from_st, to_st, distance = logistic_distance_verbose(from_station_query, to_station_query, data_root=data_root)
    except Exception:
        return None
    return from_st, to_st, int(distance)


def get_distance_km_full(from_city: str, to_city: str, data_root: str = "data") -> DistanceResult:
    """
    "Полная" схема: считаем и по Книге 1 (K1), и по Книге 2+3 (K2+K3) и берём минимум,
    если оба варианта доступны.
    """
    cand1 = _try_calc_book1(from_city, to_city, data_root=data_root)
    cand23 = _try_calc_book2_book3(from_city, to_city, data_root=data_root)

    if cand1 is None and cand23 is None:
        raise ValueError("Не удалось посчитать расстояние ни по Книге 1, ни по Книгам 2+3.")

    if cand1 is not None and cand23 is None:
        from_st, to_st, dist = cand1
        return DistanceResult(from_station=from_st, to_station=to_st, distance_km=dist, method="book1")

    if cand1 is None and cand23 is not None:
        from_st, to_st, dist = cand23
        return DistanceResult(from_station=from_st, to_station=to_st, distance_km=dist, method="book2+3")

    assert cand1 is not None and cand23 is not None
    from1, to1, d1 = cand1
    from2, to2, d2 = cand23

    if d1 <= d2:
        return DistanceResult(from_station=from1, to_station=to1, distance_km=d1, method="min(book1,book2+3)")
    return DistanceResult(from_station=from2, to_station=to2, distance_km=d2, method="min(book1,book2+3)")

