import csv
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import DefaultDict, Dict, List, Tuple


def _clean_cell(cell: str) -> str:
    if cell is None:
        return ""
    return str(cell).replace("\ufeff", "").strip().strip('"').strip()


def normalize_base_name(name: str) -> str:
    """
    Нормализация для сопоставления между Книгой 2 (обычно имя без скобок)
    и Книгой 3 (часто имя + "(код/дорога)").
    """
    s = _clean_cell(name).lower().replace("ё", "е").strip()
    # убираем суффиксы вида " [2]"
    s = re.sub(r"\s*\[\d+\]\s*$", "", s)
    # убираем " (...)" из Книги 3, чтобы совпадало по базовому имени
    s = re.sub(r"\s*\(.*?\)\s*", " ", s)
    # схлопываем пробелы
    s = re.sub(r"\s+", " ", s).strip()
    return s


@dataclass(frozen=True)
class StationTransit:
    tp_base_name: str
    distance_km: int


_ENTRY_WITH_CODE_AND_DISTANCE_RE = re.compile(
    r"(?P<code>\d{6})\s+(?P<name>.+?)\s*-\s*(?P<dist>\d+)\s*км",
    flags=re.IGNORECASE,
)


def parse_transit_list(transit_cell: str) -> List[StationTransit]:
    """
    Книга 2: в ячейке вида:
      "680000 Есиль - 255км, 406802 Подольск - 36км"
    извлекаем список (ТП, расстояние).
    """
    t = _clean_cell(transit_cell)
    if not t:
        return []

    if t.upper() == "ТП":
        # Вводно иногда ставят "ТП" вместо списка; это означает:
        # станция сама является транзитным пунктом (вызов должен добавить tp=station, dist=0).
        return []

    # Основной паттерн: код(6 цифр) + имя + '-' + расстояние + 'км'
    out: List[StationTransit] = []
    for m in _ENTRY_WITH_CODE_AND_DISTANCE_RE.finditer(t):
        name = m.group("name")
        dist = int(m.group("dist"))
        out.append(StationTransit(tp_base_name=normalize_base_name(name), distance_km=dist))

    if out:
        return out

    # Fallback: иногда может быть без кода в каждой записи (на всякий случай).
    # Пример: "Есиль - 255км, Подольск - 36км"
    simple_re = re.compile(r"(?P<name>.+?)\s*-\s*(?P<dist>\d+)\s*км", flags=re.IGNORECASE)
    for m in simple_re.finditer(t):
        name = m.group("name")
        dist = int(m.group("dist"))
        name = re.sub(r"^\s*\d{6}\s+", "", name)
        out.append(StationTransit(tp_base_name=normalize_base_name(name), distance_km=dist))

    return out


def parse_book2_part1(folder_path: str) -> Dict[str, List[StationTransit]]:
    """
    Возвращает: station_base_name -> список (tp_base_name, расстояние до него).
    Источник: data/kniga2/*.csv (часть 1: алфавитный список раздельных пунктов).
    """
    station_to_transits: DefaultDict[str, List[StationTransit]] = defaultdict(list)
    # base_name -> display_name (первое встретившееся) для UX
    station_base_to_name: Dict[str, str] = {}

    for file_name in sorted(os.listdir(folder_path)):
        if not file_name.endswith(".csv"):
            continue

        file_path = os.path.join(folder_path, file_name)
        with open(file_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                row = [_clean_cell(c) for c in row]
                if len(row) < 6:
                    continue

                station_name = row[1]
                station_code = row[5]
                transit_cell = row[4]

                # Пропускаем заголовки/мусор
                if not station_name:
                    continue
                if station_name.lower().startswith("алфавит"):
                    continue
                if not station_code or not re.fullmatch(r"\d{6}", station_code):
                    continue

                station_base = normalize_base_name(station_name)
                station_base_to_name.setdefault(station_base, station_name)

                # Если в колонке прямо "ТП" — считаем, что станция является транзитным пунктом.
                if _clean_cell(transit_cell).upper() == "ТП":
                    station_to_transits[station_base].append(StationTransit(tp_base_name=station_base, distance_km=0))
                    continue

                entries = parse_transit_list(transit_cell)
                if not entries:
                    continue
                station_to_transits[station_base].extend(entries)

    return dict(station_to_transits)


def parse_book2_part1_with_display(folder_path: str) -> Tuple[Dict[str, List[StationTransit]], Dict[str, str]]:
    """
    Аналог parse_book2_part1, но дополнительно возвращает base_name -> display_name.
    """
    station_to_transits: DefaultDict[str, List[StationTransit]] = defaultdict(list)
    station_base_to_name: Dict[str, str] = {}

    for file_name in sorted(os.listdir(folder_path)):
        if not file_name.endswith(".csv"):
            continue

        file_path = os.path.join(folder_path, file_name)
        with open(file_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                row = [_clean_cell(c) for c in row]
                if len(row) < 6:
                    continue

                station_name = row[1]
                station_code = row[5]
                transit_cell = row[4]

                if not station_name:
                    continue
                if station_name.lower().startswith("алфавит"):
                    continue
                if not station_code or not re.fullmatch(r"\d{6}", station_code):
                    continue

                station_base = normalize_base_name(station_name)
                station_base_to_name.setdefault(station_base, station_name)

                if _clean_cell(transit_cell).upper() == "ТП":
                    station_to_transits[station_base].append(
                        StationTransit(tp_base_name=station_base, distance_km=0)
                    )
                    continue

                entries = parse_transit_list(transit_cell)
                if not entries:
                    continue
                station_to_transits[station_base].extend(entries)

    return dict(station_to_transits), station_base_to_name

