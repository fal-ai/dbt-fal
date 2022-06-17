from __future__ import annotations

import argparse
import sys
import traceback
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Iterator

from dbt.logger import GLOBAL_LOGGER as logger

from fal.node_graph import FalScript
from fal.utils import print_run_info
from faldbt.project import DbtModel, FalDbt, NodeStatus

SUCCESS = 0
FAILURE = 1


class Task:
    def execute(self, args: argparse.Namespace, fal_dbt: FalDbt) -> int:
        raise NotImplementedError


def _unique_id_to_model_name(unique_id: str) -> str:
    split_list = unique_id.split(".")
    # if its a unique id 'model.fal_test.model_with_before_scripts'
    return split_list[len(split_list) - 1]


def _unique_ids_to_model_names(id_list: list[str]) -> list[str]:
    return list(map(_unique_id_to_model_name, id_list))


def _mark_dbt_nodes_status(
    fal_dbt: FalDbt,
    status: NodeStatus,
    dbt_node: str | None = None,
):
    for model in fal_dbt.models:
        if dbt_node is not None:
            if model.unique_id == dbt_node:
                model.set_status(status)
        else:
            model.set_status(status)


def _map_cli_output_model_statuses(
    run_results: dict[Any, Any]
) -> Iterator[tuple[str, NodeStatus]]:
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
    yield
    sys.path.remove(fal_dbt.scripts_dir)


@dataclass
class DBTTask(Task):
    model_ids: list[str]
    _run_index: int = -1

    def execute(self, args: argparse.Namespace, fal_dbt: FalDbt) -> int:
        from fal.cli.dbt_runner import dbt_run

        assert self._run_index != -1

        model_names = _unique_ids_to_model_names(self.model_ids)
        output = dbt_run(
            args,
            model_names,
            fal_dbt.target_path,
            self._run_index,
        )

        for node, status in _map_cli_output_model_statuses(output.run_results):
            _mark_dbt_nodes_status(fal_dbt, status, node)

        return output.return_code


@dataclass
class FalModelTask(DBTTask):
    bound_model: DbtModel | None = None

    def execute(self, args: argparse.Namespace, fal_dbt: FalDbt) -> int:
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
    hook_path: str
    bound_model: DbtModel | None = None

    def execute(self, args: argparse.Namespace, fal_dbt: FalDbt) -> int:
        script = FalScript(fal_dbt, self.bound_model, self.hook_path, True)
        return _run_script(script, fal_dbt=fal_dbt)


@dataclass
class Node:
    task: Task
    post_hooks: list[Task] = field(default_factory=list)
    dependencies: list[Node] = field(default_factory=list)

    def exit(self, status: int) -> None:
        if status != SUCCESS:
            raise RuntimeError("Error !!!")

    def set_run_index(self, run_index: int) -> None:
        if isinstance(self.task, DBTTask):
            self.task._run_index = run_index

    @cached_property
    def id(self) -> str:
        return str(uuid.uuid4())
