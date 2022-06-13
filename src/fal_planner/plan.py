import itertools
from typing import Iterator

import networkx as nx


def _find_subgraphs(graph: nx.DiGraph) -> Iterator[list[str]]:
    # Initially sort the graph, and split it
    # into chunks by using the critical node
    # approach (post-hooks are also a thing
    # now!!).

    # Then for each chunk, check whether they have the
    # same set of ancestors. This is important because
    # we want to group only the things that are in
    # the same branch.

    current_stack = []
    allowed_ancestors = set()

    def split() -> Iterator[list[str]]:
        if len(current_stack) > 1:
            yield current_stack.copy()
        current_stack.clear()
        allowed_ancestors.clear()

    for node in nx.topological_sort(graph):
        properties = graph.nodes[node]
        if properties["kind"] == "python model":
            yield from split()
            continue

        ancestors = nx.ancestors(graph, node)
        if not current_stack:
            allowed_ancestors = ancestors

        if not ancestors.issubset(allowed_ancestors):
            yield from split()

        current_stack.append(node)
        allowed_ancestors |= {node, *ancestors}

        if properties.get("post-hook"):
            yield from split()

    yield from split()


def _reduce_subgraph(
    graph: nx.DiGraph,
    nodes: list[str],
) -> None:
    subgraph = graph.subgraph(nodes).copy()

    # Use the same set of properties as the last
    # node, since only it can have post hooks.
    graph.add_node(subgraph, **graph.nodes[nodes[-1]].copy())

    for node in nodes:
        for predecessor in graph.predecessors(node):
            graph.add_edge(predecessor, subgraph)

        for successor in graph.successors(node):
            graph.add_edge(subgraph, successor)

        graph.remove_node(node)


def plan_graph(graph: nx.DiGraph) -> nx.DiGraph:
    # Implementation of Gorkem's Algorithm
    new_graph = graph.copy()

    for nodes in _find_subgraphs(new_graph):
        _reduce_subgraph(new_graph, nodes)

    return new_graph
