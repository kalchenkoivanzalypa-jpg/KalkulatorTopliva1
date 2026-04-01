import re
import heapq


def normalize_name(name):
    return str(name).lower().replace("ё", "е").strip()


def strip_suffix(name):
    """
    Убираем служебный хвост вида:
    'Москва-Рижская [2]' -> 'Москва-Рижская'
    """
    return re.sub(r"\s*\[\d+\]\s*$", "", str(name)).strip()


def canonical_name(name):
    return normalize_name(strip_suffix(name))


def is_service_point(name):
    low = canonical_name(name)

    bad_prefixes = [
        "оп ",
        "остановочный пункт",
        "пост ",
        "путевой пост",
    ]

    return any(low.startswith(x) for x in bad_prefixes)


def station_priority(name, query):
    """
    Чем МЕНЬШЕ score, тем лучше.
    """
    raw = str(name).strip()
    base = strip_suffix(raw)

    raw_norm = normalize_name(raw)
    base_norm = normalize_name(base)
    query_norm = normalize_name(query)

    score = 1000

    # 1. точное совпадение
    if base_norm == query_norm:
        score -= 700

    # 2. начинается с запроса
    if base_norm.startswith(query_norm):
        score -= 250

    # 3. просто содержит
    if query_norm in base_norm:
        score -= 120

    # 4. бонус за короткое имя
    score += len(base_norm) * 0.5

    # 5. штраф за дубль [2], [3]
    if re.search(r"\[\d+\]\s*$", raw):
        score += 40

    # 6. штраф за служебные точки
    if is_service_point(raw):
        score += 120

    # 7. штраф за редкие "технические" московские узлы при запросе "москва"
    if query_norm == "москва":
        heavy_penalty_words = [
            "сортировочная",
            "товарная",
            "киевская",
            "казанская",
            "смоленская",
            "ярославская",
            "курская",
        ]

        # Москва-Рижская / Москва-Пассажирская / Москва-Каланчевская поднимаем,
        # сортировочные и товарные опускаем
        if "рижская" in base_norm:
            score -= 180
        if "каланчевская" in base_norm:
            score -= 90
        if "пассажирская" in base_norm:
            score -= 70

        for word in heavy_penalty_words:
            if word in base_norm:
                score += 70

    # 8. для "новгород" не пихаем "новгород-северский" наверх
    if query_norm == "новгород" and "северский" in base_norm:
        score += 250

    # 9. для "хабаровск" предпочитаем I, а не II
    if query_norm == "хабаровск":
        if "хабаровск i" in base_norm:
            score -= 120
        if "хабаровск ii" in base_norm:
            score += 40

    # 10. для "тверь" обычная тверь выше дубля
    if query_norm == "тверь" and base_norm == "тверь":
        score -= 100

    return score


def find_station_candidates(graph, query, limit=10):
    query_norm = normalize_name(query)
    ranked = []

    for node in graph.keys():
        node_str = str(node)
        base = strip_suffix(node_str)

        node_norm = normalize_name(node_str)
        base_norm = normalize_name(base)

        if query_norm in node_norm or query_norm in base_norm:
            score = station_priority(node_str, query)
            ranked.append((score, len(base_norm), node_str))

    ranked.sort(key=lambda x: (x[0], x[1], x[2]))
    return [x[2] for x in ranked[:limit]]


def choose_station_interactive(graph, query):
    candidates = find_station_candidates(graph, query, limit=10)

    if not candidates:
        print(f"Станция не найдена: {query}")
        return None

    if len(candidates) == 1:
        print(f"{query} -> {candidates[0]}")
        return candidates[0]

    print(f"\nНайдено несколько вариантов для '{query}':")
    for i, station in enumerate(candidates, start=1):
        mark = "  [auto]" if i == 1 else ""
        print(f"{i}. {station}{mark}")

    choice = input("Выбери номер станции или Enter для авто: ").strip()

    if choice == "":
        return candidates[0]

    if choice.isdigit():
        idx = int(choice)
        if 1 <= idx <= len(candidates):
            return candidates[idx - 1]

    print("Некорректный ввод. Беру лучший автоподбор.")
    return candidates[0]


def shortest_path(graph, start, end):
    queue = [(0, start)]
    distances = {start: 0}
    previous = {start: None}
    visited = set()

    while queue:
        current_dist, current_node = heapq.heappop(queue)

        if current_node in visited:
            continue

        visited.add(current_node)

        if current_node == end:
            break

        for neighbor, weight in graph.get(current_node, []):
            new_dist = current_dist + weight

            if neighbor not in distances or new_dist < distances[neighbor]:
                distances[neighbor] = new_dist
                previous[neighbor] = current_node
                heapq.heappush(queue, (new_dist, neighbor))

    if end not in distances:
        return None, []

    path = []
    node = end
    while node is not None:
        path.append(node)
        node = previous.get(node)

    path.reverse()
    return distances[end], path


def get_distance_with_path(graph, start_station, end_station):
    return shortest_path(graph, start_station, end_station)


def simplify_path(path):
    if not path:
        return []

    result = []

    for node in path:
        name = str(node).strip()
        low = normalize_name(name)

        # скрываем ОП
        if low.startswith("оп "):
            continue

        # скрываем км-мусор на всякий случай
        if "км" in low:
            continue

        # подряд одинаковые по базе не дублируем
        if result and strip_suffix(result[-1]) == strip_suffix(name):
            continue

        result.append(name)

    return result