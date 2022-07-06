from __future__ import annotations

from typing import Iterator, List

import networkx as nx
from fal.node_graph import NodeKind
from fal.cli.selectors import ExecutionPlan
from dbt.logger import GLOBAL_LOGGER as logger
from dataclasses import dataclass


@dataclass
class OriginGraph:
    graph: nx.DiGraph

    def copy_graph(self) -> nx.DiGraph:
        return self.graph.copy()


@dataclass
class FilteredGraph(OriginGraph):
    graph: nx.DiGraph

    @classmethod
    def from_execution_plan(
        cls,
        origin_graph: OriginGraph,
        execution_plan: ExecutionPlan,
    ) -> FilteredGraph:
        graph = origin_graph.copy_graph()
        for node in origin_graph.graph.nodes:
            if (
                node
                not in execution_plan.dbt_models
                + execution_plan.before_scripts
                + execution_plan.after_scripts
            ):
                graph.remove_node(node)

        if execution_plan.after_scripts:
            logger.warn("Using after scripts is now deprecated. Please consider migrating to post-hooks.")

        return cls(graph)


@dataclass
class PlannedGraph(OriginGraph):
    graph: nx.DiGraph

    @classmethod
    def from_filtered_graph(
        cls,
        filtered_graph: FilteredGraph,
        enable_chunking: bool = True,
    ):
        graph = filtered_graph.copy_graph()
        planned_graph = cls(graph)
        if enable_chunking:
            planned_graph.plan()
        return planned_graph

    def _find_subgraphs(self) -> Iterator[List[str]]:
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

        for node in nx.topological_sort(self.graph):
            properties = self.graph.nodes[node]
            if properties["kind"] in (
                NodeKind.FAL_MODEL,
                NodeKind.FAL_SCRIPT
            ):
                yield from split()
                continue

            ancestors = nx.ancestors(self.graph, node)
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
        self,
        nodes: List[str],
    ) -> None:
        subgraph = self.graph.subgraph(nodes).copy()

        # Use the same set of properties as the last
        # node, since only it can have any post hooks.
        self.graph.add_node(
            subgraph,
            **self.graph.nodes[nodes[-1]].copy(),
            exit_node=nodes[-1],
        )

        for node in nodes:
            for predecessor in self.graph.predecessors(node):
                self.graph.add_edge(predecessor, subgraph)

            for successor in self.graph.successors(node):
                self.graph.add_edge(subgraph, successor)

            self.graph.remove_node(node)

    def plan(self):
        # Implementation of Gorkem's Critical Nodes Algorithm
        # with a few modifications.
        for nodes in self._find_subgraphs():
            self._reduce_subgraph(nodes)
