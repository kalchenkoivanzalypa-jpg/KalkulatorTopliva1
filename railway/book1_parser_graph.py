import csv
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import DefaultDict, Dict, List, Optional, Tuple


KM_RE = re.compile(r"(?P<km>\d+)\s*км", flags=re.IGNORECASE)
CODE_RE = re.compile(r"(?P<code>\d{6})")


def clean_cell(cell: str) -> str:
    if cell is None:
        return ""
    return str(cell).replace("\ufeff", "").strip().strip('"').strip()


def normalize_name(s: str) -> str:
    return clean_cell(s).lower().replace("ё", "е")


def _is_section_header(cell0: str) -> bool:
    # В Книге 1 заголовок участка часто выглядит как: '1) участок 43-001 "..." (Основной тарифный участок)'
    c = (cell0 or "").lower()
    return "участок" in c and ("(" in c or ")" in c or "-" in c)


def _iter_book1_rows(folder_path: str):
    for file_name in sorted(os.listdir(folder_path)):
        if not file_name.endswith(".csv"):
            continue

        file_path = os.path.join(folder_path, file_name)
        with open(file_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                yield [clean_cell(c) for c in row]


@dataclass(frozen=True)
class Node:
    code: str
    name: str


def _try_parse_node_and_coord(row: List[str]) -> Optional[Tuple[str, str, int]]:
    """
    Ожидаем структуру, типичная для Книги 1:
      col1: 6-значный код (например 440001 .)
      col2: название пункта
      col3+: значения вида '0 км' и '29 км'
    Возвращаем (code, name, coord) где coord - первое встреченное значение 'N км' в строке.
    """
    # минимум: [i, code, station_name, dist1, dist2...]
    if len(row) < 4:
        return None

    # В книге есть служебные строки; они не дадут 6-значного кода.
    code_cell = row[1] if len(row) > 1 else ""
    code_match = CODE_RE.search(code_cell)
    if not code_match:
        return None
    code = code_match.group("code")

    name = row[2] if len(row) > 2 else ""
    if not name:
        return None

    # Встречаем координату: первый числовой км из оставшихся ячеек
    coord = None
    for cell in row[3:]:
        m = KM_RE.search(cell)
        if not m:
            continue
        coord = int(m.group("km"))
        break
    if coord is None:
        return None

    return code, name, coord


def build_book1_weighted_graph(
    folder_path: str = "data/kniga1",
) -> Tuple[Dict[str, List[Tuple[str, int]]], Dict[str, str]]:
    """
    Строит граф по Книге 1 (ориентир: последовательность пунктов в пределах одного участка).

    Для каждого участка:
      - берём координату (км) из строки каждого пункта,
      - соединяем соседние по порядку строки рёбрами с весом = abs(delta_km).

    Узлы — по ЕСК-коду (6 цифр) из колонки 'Коды'.
    """
    graph: DefaultDict[str, List[Tuple[str, int]]] = defaultdict(list)
    code_to_name: Dict[str, str] = {}

    prev_code: Optional[str] = None
    prev_coord: Optional[int] = None

    for row in _iter_book1_rows(folder_path):
        cell0 = row[0] if row else ""
        if _is_section_header(cell0):
            prev_code = None
            prev_coord = None
            continue

        # пробуем извлечь (code, name, coord) из строки таблицы
        parsed = _try_parse_node_and_coord(row)
        if not parsed:
            continue

        code, name, coord = parsed
        code_to_name[code] = code_to_name.get(code, name)

        if prev_code is not None and prev_coord is not None:
            w = abs(coord - prev_coord)
            # В Книге 1 из-за округления могут встречаться соседние пункты с разницей 0 км.
            # Такие ребра нельзя выкидывать: иначе граф может стать несвязным.
            graph[prev_code].append((code, w))
            graph[code].append((prev_code, w))

        prev_code = code
        prev_coord = coord

    # Приводим к обычным dict для удобства
    return dict(graph), code_to_name


def resolve_city_to_station_code(
    city_query: str,
    code_to_name: Dict[str, str],
    pick_first: bool = True,
) -> str:
    """
    Выбираем любую станцию/пункт, у которого имя содержит запрос по городу.
    """
    q = normalize_name(city_query)
    candidates: List[Tuple[str, str]] = []
    for code, name in code_to_name.items():
        n = normalize_name(name)
        if q in n:
            candidates.append((code, name))

    if not candidates:
        raise ValueError(f"Не нашёл станции для запроса города: {city_query!r}")

    # Приоритет: точное вхождение всего слова/строки если возможно (упрощённо).
    # Если кандидат один — берем его.
    if pick_first:
        # сортировка для стабильности: сначала короче имя
        candidates.sort(key=lambda x: (len(normalize_name(x[1])), x[1]))
        return candidates[0][0]

    # fallback
    return candidates[0][0]

