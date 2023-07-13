from __future__ import annotations

from collections import defaultdict
from typing import Any
from pathlib import Path

import networkx as nx

from fal.dbt.cli.selectors import ExecutionPlan
from fal.dbt.node_graph import DbtModelNode, NodeGraph
from fal.dbt.planner.plan import (
    FilteredGraph,
    OriginGraph,
    PlannedGraph,
    ScriptConnectedGraph,
)
from fal.dbt.planner.schedule import schedule_graph
from fal.dbt.fal_script import LocalHook


class ModelDict(defaultdict):
    def get(self, key):
        return super().__getitem__(key)


class FakeDbtModel:
    @property
    def python_model(self):
        return Path("...")


def to_graph(data: list[tuple[str, dict[str, Any]]]) -> nx.DiGraph:
    graph = nx.DiGraph()

    nodes, edges = [], []
    for node, _properties in data:
        properties = _properties.copy()
        for hook_type in ["pre_hook", "post_hook"]:
            if hook_type in properties:
                properties[hook_type] = [
                    LocalHook(path) for path in properties[hook_type]
                ]

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
    connected_graph = ScriptConnectedGraph.from_filtered_graph(filtered_graph)
    planned_graph = PlannedGraph.from_script_connected_graph(
        connected_graph, enable_chunking=enable_chunking
    )
    return planned_graph.graph


def to_scheduler(graph):
    if isinstance(graph, list):
        graph = to_graph(graph)
    new_graph = plan_graph(graph, to_plan(graph))
    node_graph = NodeGraph(
        graph, ModelDict(lambda: DbtModelNode("...", FakeDbtModel()))
    )
    return schedule_graph(new_graph, node_graph)
