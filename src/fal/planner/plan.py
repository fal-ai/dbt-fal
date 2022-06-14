import copy
from typing import Iterator

import networkx as nx
from fal.node_graph import NodeKind


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
    new_graph = copy.deepcopy(graph)

    for nodes in _find_subgraphs(new_graph):
        _reduce_subgraph(new_graph, nodes)

    return new_graph


def __test_reorder_graph(graph: nx.DiGraph) -> nx.DiGraph:
    # Temporarily re-order the graph for testing purposes. Eliminates the
    # before scripts and makes all after scripts post-hooks.

    from fal.cli.selectors import _is_before_script

    new_graph = copy.deepcopy(graph)

    for node, properties in graph.nodes(data=True):
        kind = properties["kind"]
        if kind is not NodeKind.FAL_SCRIPT:
            continue

        if _is_before_script(node):
            [model] = new_graph.successors(node)
            assert len(list(new_graph.predecessors(node))) == 0
            new_graph.remove_node(node)
        else:
            [model] = new_graph.predecessors(node)
            assert len(list(graph.successors(node))) == 0
            new_graph.remove_node(node)
            new_graph.nodes[model].setdefault("post-hook", []).append(node)

    return new_graph


def _dump_graph(graph: nx.DiGraph) -> None:
    nx.nx_agraph.view_pygraphviz(graph)
