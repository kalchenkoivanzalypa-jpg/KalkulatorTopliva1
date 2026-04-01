from loader import load_csv_folder
from graph_builder import (
    build_local_graph,
    build_tp_graph,
    build_station_tp_map,
    merge_graphs,
)
from railway_service import (
    choose_station_interactive,
    get_distance_with_path,
    simplify_path,
)


def build_graph():
    print("Загружаем данные...")

    k1 = load_csv_folder("data/kniga1")

    print("Строим граф (только Книга 1)...")

    local_graph = build_local_graph(k1)
    tp_graph = build_tp_graph([])
    station_tp = build_station_tp_map([])

    graph = merge_graphs(tp_graph, local_graph, station_tp)

    print("Готово.")
    print(f"Всего узлов в графе: {len(graph)}")

    return graph


def run_single_route(graph):
    start_query = input("\nВведи станцию отправления: ").strip()
    end_query = input("Введи станцию назначения: ").strip()

    print("\nВыбор станции отправления:")
    start_station = choose_station_interactive(graph, start_query)
    if not start_station:
        print("Не удалось выбрать станцию отправления")
        return

    print("\nВыбор станции назначения:")
    end_station = choose_station_interactive(graph, end_query)
    if not end_station:
        print("Не удалось выбрать станцию назначения")
        return

    print("\nИтоговый выбор:")
    print(f"{start_query} -> {start_station}")
    print(f"{end_query} -> {end_station}")

    distance, path = get_distance_with_path(graph, start_station, end_station)

    print("\nРезультат:")
    print(distance)

    print("\nМаршрут (полный):")
    if path:
        for station in path:
            print(station)
    else:
        print("Маршрут не найден")

    print("\nМаршрут (сжатый):")
    if path:
        short_path = simplify_path(path)
        if short_path:
            print(" -> ".join(short_path))
        else:
            print("Маршрут не найден")
    else:
        print("Маршрут не найден")


def main():
    graph = build_graph()

    while True:
        print("\nЧто делаем?")
        print("1. Посчитать один маршрут")
        print("2. Выход")

        choice = input("Выбери пункт: ").strip()

        if choice == "1":
            run_single_route(graph)
        elif choice == "2":
            print("Выход.")
            break
        else:
            print("Некорректный ввод.")


if __name__ == "__main__":
    main()