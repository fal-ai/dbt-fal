from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Iterator, Any

import networkx as nx

# For testing purposes, speed everything up by this factor
SPEED_FACTOR = 10


@dataclass
class Task:
    name: str
    properties: dict[str, Any] = field(default_factory=dict)
    dependencies: list[Task] = field(default_factory=list)

    def execute(self) -> Task:
        kind = self.properties["kind"]
        if kind == "dbt model":
            action = "dbt run --select "
        else:
            action = "./fal "

        print(f"Running '{action} {self.name}'")
        time.sleep(self.properties["time"] / SPEED_FACTOR)
        print(f"Completed {self.name}!")
        return self


@dataclass
class TaskQueue:
    tasks: list[Task]
    _staged_tasks: list[Task] = field(default_factory=list)

    @property
    def _dependants(self) -> defaultdict[str, list[Task]]:
        # Task -> Dependants
        dependants = defaultdict(list)
        for task in self.tasks:
            for dependency in task.dependencies:
                dependants[dependency.name].append(task)
        return dependants

    def _stage_task(self, target_task: Task) -> None:
        self.tasks.remove(target_task)
        self._staged_tasks.append(target_task)

    def finish(self, target_task: Task) -> None:
        self._staged_tasks.remove(target_task)

        for task in self.tasks:
            if target_task in task.dependencies:
                task.dependencies.remove(target_task)

    def iter_available_tasks(self) -> Iterator[Task]:
        unblocked_tasks = [task for task in self.tasks if not task.dependencies]
        unblocked_tasks.sort(
            key=lambda task: len(self._dependants[task.name]), reverse=True
        )

        for task in unblocked_tasks:
            self._stage_task(task)
            yield task

    def get_next_task(self) -> Task | None:
        return next(self.iter_available_tasks(), None)


def load_graph(graph: nx.DiGraph) -> TaskQueue:
    tasks = {
        name: Task(name, properties) for name, properties in graph.nodes(data=True)
    }

    for name, task in tasks.items():
        task.dependencies = [tasks[ancestor] for ancestor in nx.ancestors(graph, name)]

    return TaskQueue(list(tasks.values()))
