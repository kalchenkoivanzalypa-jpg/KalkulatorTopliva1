import csv
import os
import re
from collections import defaultdict
from typing import DefaultDict, Dict, List, Optional, Tuple

from book2_parser import normalize_base_name


_INT_RE = re.compile(r"^\d+$")


def _looks_like_tp_name(s: str) -> bool:
    # В Книге 3 ТП часто имеют вид: "Новокузнецк-Восточный (83 З-Сиб)"
    s = (s or "").strip()
    return bool(s) and ("(" in s and ")" in s)


def _parse_header_targets(row: List[str]) -> Optional[List[str]]:
    """
    Ищем строку, которая содержит список конечных пунктов маршрута (ТП) по колонкам.
    Обычно первые 2 колонки пустые, а далее идут названия ТП.
    """
    if len(row) < 10:
        return None
    if (row[0] or "").strip() != "" or (row[1] or "").strip() != "":
        return None

    targets = [normalize_base_name(c) for c in row[2:] if _looks_like_tp_name(c)]
    # Мало вероятно, что это полезная таблица, если ТП совсем мало.
    if len(targets) < 30:
        return None
    return targets


def _maybe_parse_distance(cell: str) -> Optional[int]:
    cell = (cell or "").strip()
    if not cell:
        return None
    if cell == "—":
        return None
    # В CSV часто только число (в км)
    if _INT_RE.match(cell):
        return int(cell)
    return None


def parse_book3_tp_distances(folder_path: str) -> Dict[str, Dict[str, int]]:
    """
    Возвращает расстояния между транзитными пунктами:
      dist_tp[from_tp_base][to_tp_base] = км (int)
    """
    # dist_tp[from][to] = min distance (на всякий случай)
    dist_tp: DefaultDict[str, Dict[str, int]] = defaultdict(dict)

    header_targets: Optional[List[str]] = None

    for file_name in sorted(os.listdir(folder_path)):
        if not file_name.endswith(".csv"):
            continue

        file_path = os.path.join(folder_path, file_name)
        with open(file_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue

                row = [str(c).replace("\ufeff", "").strip().strip('"') for c in row]

                if header_targets is None:
                    maybe = _parse_header_targets(row)
                    if maybe is not None:
                        header_targets = maybe
                    continue

                # Если заголовок снова встретился — перезапускаем парсинг для новой таблицы.
                maybe = _parse_header_targets(row)
                if maybe is not None:
                    header_targets = maybe
                    continue

                # Ожидаем строки вида:
                # "3","Азов (51 С-Кав)","","1977","0",...
                if len(row) < 3:
                    continue
                if not (row[0] or "").strip().isdigit():
                    continue
                if not row[1]:
                    continue

                tp_from_base = normalize_base_name(row[1])

                # Колонки соответствуют targets начиная с третьей колонки исходной строки.
                # В строках-данных часто есть пустые ячейки из-за треугольного хранения.
                for col_idx in range(2, len(row)):
                    cell = row[col_idx]
                    d = _maybe_parse_distance(cell)
                    if d is None:
                        continue

                    target_idx = col_idx - 2
                    if target_idx < 0 or target_idx >= len(header_targets):
                        continue

                    tp_to_base = header_targets[target_idx]

                    prev = dist_tp[tp_from_base].get(tp_to_base)
                    if prev is None or d < prev:
                        dist_tp[tp_from_base][tp_to_base] = d
                    prev = dist_tp[tp_to_base].get(tp_from_base)
                    if prev is None or d < prev:
                        dist_tp[tp_to_base][tp_from_base] = d

    return {k: dict(v) for k, v in dist_tp.items()}

