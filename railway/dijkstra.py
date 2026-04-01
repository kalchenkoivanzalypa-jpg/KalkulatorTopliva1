import heapq


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

        for neighbor, weight in graph[current_node]:
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