from __future__ import annotations

from typing import Any

import networkx as nx


def to_graph(data: list[tuple[str, dict[str, Any]]]) -> nx.DiGraph:
    graph = nx.DiGraph()

    nodes, edges = [], []
    for node, properties in data:
        edges.extend((node, edge) for edge in properties.pop("to", []))
        nodes.append((node, properties))

    graph.add_nodes_from(nodes)
    graph.add_edges_from(edges)
    return graph
