from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterator

import networkx as nx

from fal.node_graph import DbtModelNode, NodeGraph, NodeKind
from fal.planner.tasks import SUCCESS, DBTTask, FalHookTask, FalModelTask, TaskGroup


def create_group(
    node: str | nx.DiGraph, properties: dict, node_graph: NodeGraph
) -> TaskGroup:
    kind = properties["kind"]
    if isinstance(node, nx.DiGraph):
        # When we are processing a subgraph, we need to know
        # the exit point of that graph in order to properly
        # bind the post-hooks. We'll do that by sorting each
        # node and putting the `exit_node` at the end (boolean sorting).
        model_ids = sorted(
            list(node),
            key=lambda node: node == properties["exit_node"],
        )
    else:
        model_ids = [node]

    model_node = node_graph.get_node(model_ids[-1])
    assert model_node is not None

    if kind is NodeKind.DBT_MODEL:
        task = DBTTask(model_ids=model_ids)
    else:
        assert kind is NodeKind.FAL_MODEL
        assert isinstance(model_node, DbtModelNode)
        task = FalModelTask(model_ids=model_ids, bound_model=model_node)

    post_hooks = [
        FalHookTask(
            hook_path=hook_path,
            bound_model=model_node.model,
        )
        for hook_path in properties.get("post-hook", [])
    ]
    return TaskGroup(task, post_hooks=post_hooks)


@dataclass
class Scheduler:
    groups: list[TaskGroup]
    _counter: int = 0

    def __bool__(self) -> bool:
        return bool(self.groups)

    def _calculate_score(self, target_group: TaskGroup) -> tuple[int, int]:
        # Determine the priority of the group by doing a bunch
        # of calculations. This doesn't really need to be 100% precise,
        # since if we don't have this we'll have to schedule randomly.

        # 1. Number of groups which are only waiting this group (direct dependants)
        # 2. Number of groups which are waiting this group and other groups (indirect dependants)
        # ...

        direct_dependants = 0
        indirect_dependants = 0

        for group in self.groups:
            if group is target_group:
                continue

            if any(dependency is target_group for dependency in group.dependencies):
                indirect_dependants += 1
                if len(group.dependencies) == 1:
                    direct_dependants += 1

        return (direct_dependants, indirect_dependants)

    def _stage_group(self, target_group: TaskGroup) -> None:
        # When we start running a group, we don't want to simply remove it
        # just yet (since that would unblock all of its dependencies). So
        # we'll remove it from the group queue, but still keep references to
        # it from its dependencies.

        self._counter += 1
        target_group.set_run_index(self._counter)
        self.groups.remove(target_group)

    def finish(self, target_group: TaskGroup, status: int) -> None:
        # When a staged group's execution is finished, we'll remove it
        # altogether and unblock all of its dependencies.

        if status == SUCCESS:
            self._succeed(target_group)
        else:
            self._fail(target_group)

        target_group.exit(status)

    def _fail(self, target_group: TaskGroup) -> None:
        for group in self.groups.copy():
            if target_group in group.dependencies:
                self.groups.remove(group)

    def _succeed(self, target_group: TaskGroup) -> None:
        for group in self.groups.copy():
            if target_group in group.dependencies:
                group.dependencies.remove(target_group)

    def iter_available_groups(self) -> Iterator[TaskGroup]:
        # Whenever a group is finished we'll remove that from other
        # groups' dependencies. So in here we'll find all unblocked
        # groups (groups without any dependencies) and use the scoring
        # algorithm to determine the priority of each groups (kind of like
        # a dynamic topological sort).
        unblocked_groups = [group for group in self.groups if not group.dependencies]
        unblocked_groups.sort(key=self._calculate_score, reverse=True)

        for group in unblocked_groups:
            self._stage_group(group)
            yield group


def schedule_graph(graph: nx.DiGraph, node_graph: NodeGraph) -> Scheduler:
    tasks = {
        node: create_group(node, properties, node_graph)
        for node, properties in graph.nodes(data=True)
    }

    for name, task in tasks.items():
        task.dependencies = [tasks[ancestor] for ancestor in nx.ancestors(graph, name)]

    return Scheduler(list(tasks.values()))
