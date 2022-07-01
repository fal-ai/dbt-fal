from __future__ import annotations

from typing import Iterator, List

import networkx as nx
from fal.node_graph import NodeKind


def _find_subgraphs(graph: nx.DiGraph) -> Iterator[List[str]]:
    # Initially topologically sort the graph and find nodes
    # that can be grouped together to be run as a single node
    # by using the critical node approach. All nodes within a
    # group must have the same ancestors, to avoid removing
    # existing branches.

    current_stack = []
    allowed_ancestors = set()

    def split() -> Iterator[List[str]]:
        if len(current_stack) > 1:
            yield current_stack.copy()
        current_stack.clear()
        allowed_ancestors.clear()

    for node in nx.topological_sort(graph):
        properties = graph.nodes[node]
        if properties["kind"] is NodeKind.FAL_MODEL:
            yield from split()
            continue

        ancestors = nx.ancestors(graph, node)
        if not current_stack:
            allowed_ancestors = ancestors

        if not ancestors.issubset(allowed_ancestors):
            yield from split()

        current_stack.append(node)
        allowed_ancestors |= {node, *ancestors}

        if properties.get("post_hook"):
            yield from split()

    yield from split()


def _reduce_subgraph(
    graph: nx.DiGraph,
    nodes: List[str],
) -> None:
    subgraph = graph.subgraph(nodes).copy()

    # Use the same set of properties as the last
    # node, since only it can have any post hooks.
    graph.add_node(
        subgraph,
        **graph.nodes[nodes[-1]].copy(),
        exit_node=nodes[-1],
    )

    for node in nodes:
        for predecessor in graph.predecessors(node):
            graph.add_edge(predecessor, subgraph)

        for successor in graph.successors(node):
            graph.add_edge(subgraph, successor)

        graph.remove_node(node)


def plan_graph(graph: nx.DiGraph) -> nx.DiGraph:
    # Implementation of Gorkem's Critical Nodes Algorithm
    # with a few modifications.
    new_graph = graph.copy()

    for nodes in _find_subgraphs(new_graph):
        _reduce_subgraph(new_graph, nodes)

    return new_graph


# TEMPORARY!!!
def __test_reorder_graph(graph: nx.DiGraph) -> nx.DiGraph:
    # Temporarily re-order the graph for testing purposes. Eliminates the
    # before and after scripts.

    from fal.cli.selectors import _is_before_script

    new_graph = graph.copy()

    for node, properties in graph.nodes(data=True):
        kind = properties["kind"]
        if kind is not NodeKind.FAL_SCRIPT:
            continue

        if _is_before_script(node):
            assert len(list(new_graph.predecessors(node))) == 0
        else:
            assert len(list(graph.successors(node))) == 0
        new_graph.remove_node(node)

    return new_graph
