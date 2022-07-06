import argparse
import warnings
from concurrent.futures import (
    FIRST_COMPLETED,
    Executor,
    Future,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass, field
from typing import List

from fal.planner.schedule import SUCCESS, Scheduler
from fal.planner.tasks import FalHookTask, TaskGroup, Task, GroupStatus, DBTTask
from faldbt.project import FalDbt

from dbt.logger import GLOBAL_LOGGER as logger


# HACK: Since we construct multiprocessing pools for each DBT run, it leaves a trace
# of shared memory warnings behind. In reality, there isn't anything we can do to
# get rid of them since everything is closed properly and gets destroyed at the end.
# As of now, it is just a known problem of using multiprocessing like this, and for
# not spamming the users with these unrelated warnings we'll filter them out.
#
# See for more: https://stackoverflow.com/a/63004750
warnings.filterwarnings("ignore", category=UserWarning, module="multiprocessing.resource_tracker")


def _collect_models(groups: List[TaskGroup]) -> List[str]:
    for group in groups:
        if not isinstance(group.task, DBTTask):
            continue

        yield from group.task.model_ids


def _show_failed_groups(scheduler: Scheduler) -> None:
    failed_models = _collect_models(scheduler.filter_groups(GroupStatus.FAILURE))
    if failed_models:
        message = ", ".join(failed_models)
        logger.info(f"Failed calculating the following DBT models: {message}")

    skipped_models = _collect_models(scheduler.filter_groups(GroupStatus.SKIPPED))
    if skipped_models:
        message = ", ".join(skipped_models)
        logger.info(f"Skipped calculating the following DBT models: {message}")


@dataclass
class FutureGroup:
    args: argparse.Namespace
    fal_dbt: FalDbt
    task_group: TaskGroup
    executor: Executor
    futures: List[Future] = field(default_factory=list)
    status: int = SUCCESS

    def __post_init__(self) -> None:
        # In the case of us having pre-hooks, this is
        # where we'll trigger them (and handle the task_group.task)
        # below.
        self._add_tasks(self.task_group.task)

    def process(self, future: Future) -> None:
        assert future in self.futures

        self.futures.remove(future)
        self.status |= future.result()
        if not isinstance(future.task, FalHookTask):
            # Once the main task gets completed, we'll populate
            # the task queue with the post-hooks.
            self._add_tasks(*self.task_group.post_hooks)

    def _add_tasks(self, *tasks: Task) -> None:
        for task in tasks:
            future = self.executor.submit(
                task.execute,
                args=self.args,
                fal_dbt=self.fal_dbt,
            )
            future.task, future.group = task, self
            self.futures.append(future)

    @property
    def is_done(self) -> int:
        return len(self.futures) == 0


def parallel_executor(
    args: argparse.Namespace,
    fal_dbt: FalDbt,
    scheduler: Scheduler,
    max_threads: int,
) -> None:
    def get_futures(future_groups):
        return {
            # Unpack all running futures into a single set
            # to be consumed by wait().
            future
            for future_group in future_groups
            for future in future_group.futures
        }

    def create_futures(executor: ThreadPoolExecutor) -> List[FutureGroup]:
        return [
            # FutureGroup's are the secondary layer of the executor,
            # managing the parallelization of tasks.
            FutureGroup(
                args,
                fal_dbt,
                task_group=task_group,
                executor=executor,
            )
            for task_group in scheduler.iter_available_groups()
        ]

    with ThreadPoolExecutor(max_threads) as executor:
        future_groups = create_futures(executor)
        futures = get_futures(future_groups)
        while futures:
            # Get the first completed futures, mark them done.
            completed_futures, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in completed_futures:
                group: FutureGroup = future.group
                group.process(future)
                if group.is_done:
                    scheduler.finish(group.task_group, status=group.status)

            # And load all the tasks that were blocked by those futures.
            future_groups.extend(create_futures(executor))
            futures = get_futures(future_groups)

    _show_failed_groups(scheduler)
