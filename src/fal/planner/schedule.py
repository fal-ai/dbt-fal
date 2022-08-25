from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, List

import networkx as nx

from faldbt.project import DbtModel
from fal.node_graph import DbtModelNode, NodeGraph, NodeKind, ScriptNode
from fal.planner.tasks import (
    SUCCESS,
    Task,
    DBTTask,
    FalIsolatedHookTask,
    FalLocalHookTask,
    FalModelTask,
    TaskGroup,
    Status,
    HookType,
)
from fal.utils import DynamicIndexProvider
from fal.fal_script import Hook, LocalHook, IsolatedHook, create_hook


def create_hook_task(
    hook: Hook,
    bound_model: DbtModel,
    hook_type: HookType = HookType.HOOK,
) -> Task:
    local_hook = FalLocalHookTask(
        Path(hook.path),
        bound_model=bound_model,
        arguments=hook.arguments,
        hook_type=hook_type,
    )
    if isinstance(hook, IsolatedHook):
        return FalIsolatedHookTask(
            hook.environment_name,
            local_hook,
        )
    else:
        return local_hook


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

    bound_model_name = model_ids[-1]
    flow_node = node_graph.get_node(bound_model_name)

    if kind is NodeKind.DBT_MODEL:
        task = DBTTask(model_ids=model_ids)
    elif kind is NodeKind.FAL_MODEL:
        assert isinstance(flow_node, DbtModelNode)
        model_script = create_hook(
            {
                "path": str(flow_node.model.python_model),
                "environment": properties.get("environment"),
            }
        )
        model_script_task = create_hook_task(
            model_script,
            flow_node.model,
            hook_type=HookType.MODEL_SCRIPT,
        )
        task = FalModelTask(model_ids=model_ids, script=model_script_task)
    else:
        assert kind is NodeKind.FAL_SCRIPT
        assert isinstance(flow_node, ScriptNode)
        task = FalLocalHookTask.from_fal_script(flow_node.script)

    pre_hooks = properties.get("pre_hook", [])
    post_hooks = properties.get("post_hook", [])
    if pre_hooks or post_hooks:
        assert flow_node, "hook nodes must be attached to a model node"
        assert isinstance(flow_node, DbtModelNode)

    pre_hook_tasks = [
        create_hook_task(hook=pre_hook, bound_model=flow_node.model)
        for pre_hook in pre_hooks
    ]
    post_hook_tasks = [
        create_hook_task(hook=post_hook, bound_model=flow_node.model)
        for post_hook in post_hooks
    ]

    return TaskGroup(task, pre_hooks=pre_hook_tasks, post_hooks=post_hook_tasks)


@dataclass
class Scheduler:
    groups: List[TaskGroup]

    def __post_init__(self) -> None:
        index_provider = DynamicIndexProvider()
        for group in self.groups:
            for task in group.iter_tasks():
                task.set_run_index(index_provider)

    def filter_groups(self, status: Status) -> List[TaskGroup]:
        return [group for group in self.groups if group.status is status]

    @property
    def pending_groups(self) -> List[TaskGroup]:
        return self.filter_groups(Status.PENDING)

    def __bool__(self) -> bool:
        return bool(self.pending_groups)

    def _calculate_score(self, target_group: TaskGroup) -> tuple[int, int]:
        # Determine the priority of the group by doing a bunch
        # of calculations. This doesn't really need to be 100% precise,
        # since if we don't have this we'll have to schedule randomly.

        # 1. Number of groups which are only waiting this group (direct dependants)
        # 2. Number of groups which are waiting this group and other groups (indirect dependants)
        # ...

        direct_dependants = 0
        indirect_dependants = 0

        for group in self.pending_groups:
            if group is target_group:
                continue

            if any(dependency is target_group for dependency in group.dependencies):
                indirect_dependants += 1
                if len(group.dependencies) == 1:
                    direct_dependants += 1

        return (direct_dependants, indirect_dependants)

    def _stage_group(self, target_group: TaskGroup) -> None:
        target_group.status = Status.RUNNING

    def finish(self, target_group: TaskGroup, status: int) -> None:
        # When a staged group's execution is finished, we'll remove it
        # altogether and unblock all of its dependencies.

        if status == SUCCESS:
            self._succeed(target_group)
        else:
            self._fail(target_group)

    def _fail(self, target_group: TaskGroup) -> None:
        target_group.status = Status.FAILURE
        for group in self.pending_groups:
            if target_group in group.dependencies:
                group.status = Status.SKIPPED

    def _succeed(self, target_group: TaskGroup) -> None:
        target_group.status = Status.SUCCESS
        for group in self.pending_groups.copy():
            if target_group in group.dependencies:
                group.dependencies.remove(target_group)

    def iter_available_groups(self) -> Iterator[TaskGroup]:
        # Whenever a group is finished we'll remove that from other
        # groups' dependencies. So in here we'll find all unblocked
        # groups (groups without any dependencies) and use the scoring
        # algorithm to determine the priority of each groups (kind of like
        # a dynamic topological sort).
        unblocked_groups = [
            group for group in self.pending_groups if not group.dependencies
        ]
        unblocked_groups.sort(key=self._calculate_score, reverse=True)

        for group in unblocked_groups:
            self._stage_group(group)
            yield group


def schedule_graph(graph: nx.DiGraph, node_graph: NodeGraph) -> Scheduler:
    task_groups = {
        node: create_group(node, properties, node_graph)
        for node, properties in graph.nodes(data=True)
    }

    for name, task_group in task_groups.items():
        task_group.dependencies = [
            task_groups[ancestor] for ancestor in nx.ancestors(graph, name)
        ]

    return Scheduler(list(task_groups.values()))
