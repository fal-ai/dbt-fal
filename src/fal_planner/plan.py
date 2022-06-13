from __future__ import annotations

import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import IntEnum
from functools import cached_property
from typing import Any, Iterator

import networkx as nx

# For testing purposes, each task takes 0.2 seconds
TIMEOUT = 0.2
DBT_OVERHEAD = 0.05

SUCCESS = 0
FAILURE = 1


class Task:
    def execute(self) -> int:
        raise NotImplementedError


@dataclass
class DBTTask(Task):
    selectors: list[str]

    def execute(self) -> int:
        print(f"$ dbt run --select {' '.join(self.selectors)}")
        time.sleep(DBT_OVERHEAD + TIMEOUT * len(self.selectors))
        print("(DONE) $ dbt run --select {}".format(" ".join(self.selectors)))
        return SUCCESS


@dataclass
class FalModelTask(Task):
    model_name: str

    def execute(self) -> int:
        print(f"$ fal run {self.model_name}")
        time.sleep(TIMEOUT)
        print(f"(DONE) $ fal run {self.model_name}")
        return SUCCESS


@dataclass
class FalHookTask(Task):
    hook_name: str

    def execute(self) -> int:
        print(f"$ ./scripts/{self.hook_name}.py")
        time.sleep(TIMEOUT)
        print(f"(DONE) $ ./scripts/{self.hook_name}.py")
        return SUCCESS


@dataclass
class Node:
    task: Task
    post_hooks: list[Task]
    dependencies: list[Node] = field(default_factory=list)

    @cached_property
    def id(self) -> str:
        return str(uuid.uuid4())


def create_node(name: str, properties: dict) -> Node:
    if properties["kind"] == "dbt model":
        task = DBTTask(selectors=[name])
    else:
        assert properties["kind"] == "python model"
        task = FalModelTask(model_name=name)

    post_hooks = [
        FalHookTask(hook_name=hook_name)
        for hook_name in properties.get("post-hook", [])
    ]
    return Node(task, post_hooks=post_hooks)


@dataclass
class NodeQueue:
    nodes: list[Node]
    _staged_nodes: list[Node] = field(default_factory=list)

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

        self.nodes.remove(target_node)
        self._staged_nodes.append(target_node)

    def finish(self, target_node: Node, status: int) -> None:
        # When a staged node's execution is finished, we'll remove it
        # alltogether and unblock all of its dependencies.
        self._staged_nodes.remove(target_node)

        assert status == SUCCESS
        for node in self.nodes:
            if target_node in node.dependencies:
                node.dependencies.remove(target_node)

    def iter_available_nodes(self) -> Iterator[Node]:
        # Find all unblocked nodes (nodes without any dependencies) and
        # use the node score algorithm to determine the priority of each
        # node.
        unblocked_nodes = [node for node in self.nodes if not node.dependencies]
        unblocked_nodes.sort(key=self._calculate_node_score, reverse=True)

        for node in unblocked_nodes:
            if self.can_plan_further(node):
                pass
            self._stage_node(node)
            yield node


def load_graph(graph: nx.DiGraph) -> NodeQueue:
    tasks = {
        name: create_node(name, properties)
        for name, properties in graph.nodes(data=True)
    }

    for name, task in tasks.items():
        task.dependencies = [tasks[ancestor] for ancestor in nx.ancestors(graph, name)]

    return NodeQueue(list(tasks.values()))
