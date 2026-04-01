def normalize_name(name):
    return name.lower().replace("ё", "е").strip()

    def find_station(graph, query):
    query = normalize_name(query)

    matches = []

    for node in graph:
        name = normalize_name(node)

        if query in name:
            matches.append(node)

    # если нашли что-то — берём первое
    if matches:
        return matches[0]

    return None