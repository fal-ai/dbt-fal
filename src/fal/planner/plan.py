from __future__ import annotations

from typing import Callable, Iterator, List, Set

import networkx as nx
from fal.node_graph import NodeKind
from fal.cli.selectors import ExecutionPlan, _is_before_script, _is_after_script
from faldbt.logger import LOGGER
from dataclasses import dataclass


@dataclass
class OriginGraph:
    graph: nx.DiGraph

    def copy_graph(self) -> nx.DiGraph:
        return self.graph.copy()  # type: ignore

    def _plot(self, graph=None):
        """
        For development and debugging purposes
        """
        if not graph:
            graph = self.graph

        import matplotlib.pyplot as plt

        import networkx.drawing.layout as layout

        nx.draw_networkx(
            graph,
            arrows=True,
            pos=layout.circular_layout(graph),
            labels={
                node: node.replace(".", "\n")
                .replace("model\n", "")
                .replace("script\n", "")
                .replace("\npy", ".py")
                for node in graph.nodes
            },
        )
        plt.show()


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
            if node not in execution_plan.nodes:
                graph.remove_node(node)

        if execution_plan.after_scripts or execution_plan.before_scripts:
            LOGGER.warn(
                "Using before/after scripts are now deprecated. "
                "Please consider migrating to pre-hooks/post-hooks or Python models."
            )

        return cls(graph)


@dataclass
class ScriptConnectedGraph(OriginGraph):
    graph: nx.DiGraph

    @classmethod
    def from_filtered_graph(
        cls,
        filtered_graph: FilteredGraph,
    ):
        graph = filtered_graph.copy_graph()
        shuffled_graph = cls(graph)
        shuffled_graph._shuffle()
        return shuffled_graph

    def _shuffle(self):
        def _pattern_matching(node_check: Callable[[str], bool], nodes: Set[str]):
            matched = {
                maybe_script for maybe_script in nodes if node_check(maybe_script)
            }
            return matched, nodes - matched

        def get_before_scripts(graph: nx.DiGraph, node: str):
            return _pattern_matching(_is_before_script, set(graph.predecessors(node)))

        def get_after_scripts(graph: nx.DiGraph, node: str):
            return _pattern_matching(_is_after_script, set(graph.successors(node)))

        def _add_edges_from_to(from_nodes: Set[str], to_nodes: Set[str]):
            self.graph.add_edges_from(
                (from_n, to_n) for from_n in from_nodes for to_n in to_nodes
            )

        old_graph = self.copy_graph()
        node: str
        for node in old_graph.nodes:

            after_scripts, other_succs = get_after_scripts(old_graph, node)
            # Keep the original node to succs edges and add a new one from the script to succs
            _add_edges_from_to(after_scripts, other_succs)

            before_scripts, other_preds = get_before_scripts(old_graph, node)
            # Keep the original preds to node edge and add a new one from preds to the scripts
            _add_edges_from_to(other_preds, before_scripts)

            # Add edges between node's after and succ's before scripts
            for succ in other_succs:
                succ_before_scripts, _succ_other_preds = get_before_scripts(
                    old_graph, succ
                )

                # Add edge between all after scripts to the succ's before scripts
                _add_edges_from_to(after_scripts, succ_before_scripts)


@dataclass
class PlannedGraph(OriginGraph):
    graph: nx.DiGraph

    @classmethod
    def from_script_connected_graph(
        cls,
        shuffled_graph: ScriptConnectedGraph,
        enable_chunking: bool = True,
    ):
        graph = shuffled_graph.copy_graph()
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
                NodeKind.FAL_SCRIPT,
            ) or properties.get("pre_hook"):
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
