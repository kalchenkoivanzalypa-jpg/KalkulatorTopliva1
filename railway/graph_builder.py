from collections import defaultdict
import csv
import re


# =========================
# НОРМАЛИЗАЦИЯ
# =========================
def normalize_row(row):
    if not row:
        return []

    if len(row) == 1 and "," in str(row[0]):
        try:
            parsed = next(csv.reader([row[0]], delimiter=",", quotechar='"'))
            return [str(x).strip() for x in parsed]
        except Exception:
            return [str(x).strip() for x in row]

    return [str(x).strip() for x in row]


# =========================
# ФИЛЬТР СТАНЦИЙ
# =========================
def is_station(text):
    if not text:
        return False

    text = str(text).strip()
    text_lower = text.lower()

    # мусорные типы
    if "(перев" in text_lower:
        return False
    if "(эксп" in text_lower:
        return False

    if len(text) < 2:
        return False

    # чистые числа
    if text.replace(" ", "").isdigit():
        return False

    # километры
    if "км" in text_lower:
        return False

    # только цифры/точки/пробелы
    if re.fullmatch(r"[\d\.\s]+", text):
        return False

    # мусор
    if text in ["-", "", "—"]:
        return False

    # служебка
    forbidden = [
        "№ п/п",
        "код",
        "коды",
        "итого",
        "всего",
        "участок",
        "направление",
        "от станции",
        "до станции",
        "от ст",
        "до ст",
        "через",
        "таблица",
        "расстояния",
    ]
    for word in forbidden:
        if word in text_lower:
            return False

    # даты
    if any(day in text_lower for day in [
        "понедельник", "вторник", "среда",
        "четверг", "пятница", "суббота", "воскресенье"
    ]):
        return False

    if re.search(r"\d{4}", text):
        return False

    return any(c.isalpha() for c in text)


def is_section_header(text):
    if not text:
        return False

    text = str(text).upper()

    return (
        "УЧАСТОК" in text
        or "РАССТОЯНИЯ МЕЖДУ СТАНЦИЯМИ" in text
    )


def is_bad_row(row):
    joined = " ".join(row).lower()

    bad_markers = [
        "расстояния",
        "таблица",
        "итого",
        "коды",
    ]

    return any(x in joined for x in bad_markers)


# =========================
# УНИКАЛИЗАЦИЯ СТАНЦИЙ
# =========================
def make_station_label(station_name, section_id, section_station_labels, global_name_count):
    """
    Одинаковые названия из разных участков НЕ должны склеиваться.
    Внутри одного участка одна и та же станция получает один и тот же label.
    """
    key = (section_id, station_name)

    if key in section_station_labels:
        return section_station_labels[key]

    global_name_count[station_name] += 1
    count = global_name_count[station_name]

    if count == 1:
        label = station_name
    else:
        label = f"{station_name} [{count}]"

    section_station_labels[key] = label
    return label


# =========================
# ГРАФ КНИГИ 1
# =========================
def build_local_graph(k1_data):
    graph = defaultdict(list)

    # для статистики — реальные имена станций, без suffix
    total_station_names = set()

    prev_station_label = None
    section_id = 0

    # station_name -> сколько раз уже встречали глобально
    global_name_count = defaultdict(int)

    # (section_id, station_name) -> уникальный label
    section_station_labels = {}

    for raw_row in k1_data:
        row = normalize_row(raw_row)

        if not row:
            prev_station_label = None
            continue

        first_cell = row[0]

        if is_section_header(first_cell):
            section_id += 1
            prev_station_label = None
            continue

        if is_bad_row(row):
            prev_station_label = None
            continue

        current_station_name = None

        # берём первую нормальную станцию в строке
        for cell in row:
            if is_station(cell):
                current_station_name = cell.strip()
                break

        if not current_station_name:
            prev_station_label = None
            continue

        total_station_names.add(current_station_name)

        current_station_label = make_station_label(
            current_station_name,
            section_id,
            section_station_labels,
            global_name_count,
        )

        if prev_station_label and prev_station_label != current_station_label:
            graph[prev_station_label].append((current_station_label, 1))
            graph[current_station_label].append((prev_station_label, 1))

        prev_station_label = current_station_label

    print(f"Локальный граф: {len(total_station_names)} станций")
    return graph


# =========================
# ПОКА ОТКЛЮЧЕНО
# =========================
def build_tp_graph(rows):
    print("TP граф отключён")
    return {}


def build_station_tp_map(rows):
    print("Книга 2 отключена")
    return {}


# =========================
# ОБЪЕДИНЕНИЕ
# =========================
def merge_graphs(tp_graph, local_graph, station_tp):
    graph = {}

    for node in local_graph:
        graph[node] = list(local_graph[node])

    print(f"Общий граф: {len(graph)} узлов")
    return graph