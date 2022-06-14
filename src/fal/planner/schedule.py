from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator

import networkx as nx
from fal.node_graph import NodeGraph
from fal.planner.tasks import SUCCESS, DBTTask, FalHookTask, FalModelTask, Node


def create_node(
    node: str | nx.DiGraph, properties: dict, node_graph: NodeGraph
) -> Node:
    kind = properties["kind"]

    model_ids = [node]
    if kind == "dbt model":
        if isinstance(node, nx.DiGraph):
            model_ids = sorted(
                list(node),
                key=lambda node: node == properties["exit_node"],
            )
        task = DBTTask(model_ids=model_ids)
    else:
        assert isinstance(node, str)
        task = FalModelTask(model_name=node)

    model_node = node_graph.get_node(model_ids[-1])
    assert model_node is not None

    post_hooks = [
        FalHookTask(
            hook_path=hook_path,
            bound_model=model_node.model,
        )
        for hook_path in properties.get("post-hook", [])
    ]
    return Node(task, post_hooks=post_hooks)


@dataclass
class NodeQueue:
    nodes: list[Node]
    _staged_nodes: list[Node] = field(default_factory=list)
    _counter: int = 0

    def _calculate_node_score(self, target_node: Node) -> tuple[int, int]:
        # Determine the priority of the node by doing a bunch
        # of calculations. This doesn't really need to be 100% precise,
        # since if we don't have this we'll have to schedule randomly.

        # 1. Number of nodes which are only waiting this node (direct dependants)
        # 2. Number of nodes which are waiting this node and other nodes (indirect dependants)
        # ...

        direct_dependants = 0
        indirect_dependants = 1

        for node in self.nodes:
            if node is target_node:
                continue

            if any(dependency is target_node for dependency in node.dependencies):
                indirect_dependants += 1
                if len(node.dependencies) == 1:
                    direct_dependants += 1

        return (direct_dependants, indirect_dependants)

    def _stage_node(self, target_node: Node) -> None:
        # When we start running a node, we don't want to simply remove it
        # just yet (since that would unblock all of its dependencies). So
        # we temporarily add it to a separate group of nodes (staged).

        self._counter += 1
        target_node.set_run_index(self._counter)

        self.nodes.remove(target_node)
        self._staged_nodes.append(target_node)

    def finish(self, target_node: Node, status: int) -> None:
        # When a staged node's execution is finished, we'll remove it
        # alltogether and unblock all of its dependencies.
        self._staged_nodes.remove(target_node)

        if status == SUCCESS:
            self._succeed(target_node)
        else:
            self._fail(target_node)

        target_node.exit(status)

    def _fail(self, target_node: Node) -> None:
        for node in self.nodes.copy():
            if target_node in node.dependencies:
                self.nodes.remove(node)

    def _succeed(self, target_node: Node) -> None:
        for node in self.nodes.copy():
            if target_node in node.dependencies:
                node.dependencies.remove(target_node)

    def iter_available_nodes(self) -> Iterator[Node]:
        # Find all unblocked nodes (nodes without any dependencies) and
        # use the node score algorithm to determine the priority of each
        # node.
        unblocked_nodes = [node for node in self.nodes if not node.dependencies]
        unblocked_nodes.sort(key=self._calculate_node_score, reverse=True)

        for node in unblocked_nodes:
            self._stage_node(node)
            yield node


def schedule_graph(graph: nx.DiGraph, node_graph: NodeGraph) -> NodeQueue:
    tasks = {
        node: create_node(node, properties, node_graph)
        for node, properties in graph.nodes(data=True)
    }

    for name, task in tasks.items():
        task.dependencies = [tasks[ancestor] for ancestor in nx.ancestors(graph, name)]

    return NodeQueue(list(tasks.values()))
