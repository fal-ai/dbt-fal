from __future__ import annotations

import argparse
from pathlib import Path
import sys
import traceback
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Iterator, List, Any, Optional, Dict, Tuple

from dbt.logger import GLOBAL_LOGGER as logger

from fal.node_graph import FalScript
from fal.utils import print_run_info
from faldbt.project import DbtModel, FalDbt, NodeStatus

SUCCESS = 0
FAILURE = 1


class Task:
    _run_index: int = -1

    def execute(self, args: argparse.Namespace, fal_dbt: FalDbt) -> int:
        raise NotImplementedError


class GroupStatus(Enum):
    PENDING = auto()
    RUNNING = auto()
    SKIPPED = auto()
    SUCCESS = auto()
    FAILURE = auto()


def _unique_id_to_model_name(unique_id: str) -> str:
    split_list = unique_id.split(".")
    # if its a unique id 'model.fal_test.model_with_before_scripts'
    return split_list[len(split_list) - 1]


def _unique_ids_to_model_names(id_list: List[str]) -> List[str]:
    return list(map(_unique_id_to_model_name, id_list))


def _mark_dbt_nodes_status(
    fal_dbt: FalDbt,
    status: NodeStatus,
    dbt_node: Optional[str] = None,
):
    for model in fal_dbt.models:
        if dbt_node is not None:
            if model.unique_id == dbt_node:
                model.status = status
        else:
            model.status = status


def _map_cli_output_model_statuses(
    run_results: Dict[Any, Any]
) -> Iterator[Tuple[str, NodeStatus]]:
    if not isinstance(run_results.get("results"), list):
        raise Exception("Could not read dbt run results")

    for result in run_results["results"]:
        if not result.get("unique_id") or not result.get("status"):
            continue

        yield result["unique_id"], NodeStatus(result["status"])


def _run_script(script: FalScript, fal_dbt: FalDbt):
    print_run_info([script])
    logger.debug("Running script {}", script.id)
    try:
        with _modify_path(fal_dbt):
            script.exec(fal_dbt)
    except:
        logger.error("Error in script {}:\n{}", script.id, traceback.format_exc())
        # TODO: what else to do?
        status = FAILURE
    else:
        status = SUCCESS
    finally:
        logger.debug("Finished script {}", script.id)

    return status


@contextmanager
def _modify_path(fal_dbt: FalDbt):
    sys.path.append(fal_dbt.scripts_dir)
    try:
        yield
    finally:
        sys.path.remove(fal_dbt.scripts_dir)


@dataclass
class DBTTask(Task):
    model_ids: List[str]

    def execute(self, args: argparse.Namespace, fal_dbt: FalDbt) -> int:
        from fal.cli.dbt_runner import dbt_run_through_python

        assert self._run_index != -1

        model_names = _unique_ids_to_model_names(self.model_ids)
        output = dbt_run_through_python(
            args, model_names, fal_dbt.target_path, self._run_index
        )

        for node, status in _map_cli_output_model_statuses(output.run_results):
            _mark_dbt_nodes_status(fal_dbt, status, node)

        return output.return_code


@dataclass
class FalModelTask(DBTTask):
    bound_model: Optional[DbtModel] = None

    def execute(self, args: argparse.Namespace, fal_dbt: FalDbt) -> int:
        assert self._run_index != -1

        # Run the ephemeral model
        dbt_result = super().execute(args, fal_dbt)

        # And then run the Python script if it didn't fail.
        if dbt_result != SUCCESS:
            return dbt_result

        assert self.bound_model is not None
        bound_script = FalScript.model_script(fal_dbt, self.bound_model)
        script_result = _run_script(bound_script, fal_dbt=fal_dbt)

        status = NodeStatus.Success if script_result == SUCCESS else NodeStatus.Error
        _mark_dbt_nodes_status(fal_dbt, status, self.bound_model.unique_id)
        return script_result


@dataclass
class FalHookTask(Task):
    hook_path: Path
    bound_model: Optional[DbtModel] = None
    is_post_hook: bool = True

    def execute(self, args: argparse.Namespace, fal_dbt: FalDbt) -> int:
        if not self.is_post_hook:
            # For after/before scripts
            assert self._run_index != -1

        script = FalScript(
            fal_dbt, self.bound_model, str(self.hook_path), self.is_post_hook
        )
        return _run_script(script, fal_dbt=fal_dbt)


@dataclass
class TaskGroup:
    task: Task
    post_hooks: List[FalHookTask] = field(default_factory=list)
    dependencies: List[TaskGroup] = field(default_factory=list)
    status: GroupStatus = GroupStatus.PENDING

    def __post_init__(self):
        self._id = str(uuid.uuid4())

    def set_run_index(self, run_index: int) -> None:
        self.task._run_index = run_index

    @property
    def id(self) -> str:
        return self._id
