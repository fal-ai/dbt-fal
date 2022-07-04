from __future__ import annotations

from typing import Any

import networkx as nx
from fal.cli.selectors import ExecutionPlan
from fal.planner.plan import OriginGraph, FilteredGraph, PlannedGraph


def to_graph(data: list[tuple[str, dict[str, Any]]]) -> nx.DiGraph:
    graph = nx.DiGraph()

    nodes, edges = [], []
    for node, _properties in data:
        properties = _properties.copy()
        edges.extend((node, edge) for edge in properties.pop("to", []))
        nodes.append((node, properties))

    graph.add_nodes_from(nodes)
    graph.add_edges_from(edges)
    return graph


def to_plan(graph: nx.DiGraph) -> ExecutionPlan:
    return ExecutionPlan(list(graph.nodes), "<test>")


def plan_graph(
    graph: nx.DiGraph, execution_plan: ExecutionPlan, enable_chunking: bool = True
) -> nx.DiGraph:
    origin_graph = OriginGraph(graph)
    filtered_graph = FilteredGraph.from_execution_plan(
        origin_graph, execution_plan=execution_plan
    )
    planned_graph = PlannedGraph.from_filtered_graph(
        filtered_graph, enable_chunking=enable_chunking
    )
    return planned_graph.graph
