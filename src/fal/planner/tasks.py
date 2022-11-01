from __future__ import annotations

import argparse
import threading
import json
from pathlib import Path
import sys
import traceback
import uuid
from functools import partial
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Iterator, List, Any, Optional, Dict, Tuple, Union

from faldbt.logger import LOGGER

from fal.node_graph import FalScript
from fal.utils import print_run_info, DynamicIndexProvider
from faldbt.project import DbtModel, FalDbt, NodeStatus

from datetime import datetime, timezone

SUCCESS = 0
FAILURE = 1


class Task:
    def set_run_index(self, index_provider: DynamicIndexProvider) -> None:
        self._run_index = index_provider.next()

    @property
    def run_index(self) -> int:
        run_index = getattr(self, "_run_index", None)
        assert run_index is not None
        return run_index

    def execute(self, args: argparse.Namespace, fal_dbt: FalDbt) -> int:
        raise NotImplementedError


class HookType(Enum):
    HOOK = "HOOK"
    SCRIPT = "SCRIPT"
    MODEL_SCRIPT = "MODEL_SCRIPT"


class Status(Enum):
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


def _mark_dbt_nodes_status_and_response(
    fal_dbt: FalDbt,
    status: NodeStatus,
    dbt_node: Optional[str] = None,
    adapter_response: Optional[dict] = None,
):
    for model in fal_dbt.models:
        if dbt_node is not None:
            if model.unique_id == dbt_node:
                model.status = status

                if adapter_response is not None:
                    model.adapter_response = adapter_response
        else:
            model.status = status


def _map_cli_output_model_results(
    run_results: Dict[Any, Any]
) -> Iterator[Tuple[str, NodeStatus, Optional[dict]]]:
    if not isinstance(run_results.get("results"), list):
        raise Exception("Could not read dbt run results")

    for result in run_results["results"]:
        if not result.get("unique_id") or not result.get("status"):
            continue

        yield result["unique_id"], NodeStatus(result["status"]), result.get(
            "adapter_response"
        )


def _run_script(script: FalScript) -> Dict[str, Any]:
    print_run_info([script])

    # DBT seems to be dealing with only UTC times
    # so we'll follow that convention.
    started_at = datetime.now(tz=timezone.utc)
    try:
        with _modify_path(script.faldbt):
            script.exec()
    except:
        LOGGER.error("Error in script {}:\n{}", script.id, traceback.format_exc())
        # TODO: what else to do?
        status = NodeStatus.Fail
    else:
        status = NodeStatus.Success
    finally:
        LOGGER.debug("Finished script {}", script.id)
        finished_at = datetime.now(tz=timezone.utc)

    return {
        "path": str(script.path),
        "unique_id": str(script.relative_path),
        "status": status,
        "thread_id": threading.current_thread().name,
        "is_hook": script.is_hook,
        "execution_time": (finished_at - started_at).total_seconds(),
        "timing": [
            {
                "name": "execute",
                # DBT's suffix for UTC is Z, but isoformat() uses +00:00. So
                # we'll manually cast it to the proper format.
                # https://stackoverflow.com/a/42777551
                "started_at": started_at.isoformat().replace("+00:00", "Z"),
                "finished_at": finished_at.isoformat().replace("+00:00", "Z"),
            }
        ],
    }


def run_script(script: FalScript, run_index: int) -> int:
    results = _run_script(script)
    run_results_file = Path(script.faldbt.target_path) / f"fal_results_{run_index}.json"
    with open(run_results_file, "w") as stream:
        json.dump(
            {
                "results": [results],
                "elapsed_time": results["execution_time"],
            },
            stream,
        )
    return SUCCESS if results["status"] == NodeStatus.Success else FAILURE


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

        model_names = _unique_ids_to_model_names(self.model_ids)
        output = dbt_run_through_python(
            args, model_names, fal_dbt.target_path, self.run_index
        )

        for node, status, adapter_response in _map_cli_output_model_results(
            output.run_results
        ):
            _mark_dbt_nodes_status_and_response(fal_dbt, status, node, adapter_response)

        return output.return_code


@dataclass
class FalModelTask(DBTTask):
    script: Union[FalLocalHookTask, FalIsolatedHookTask]

    def set_run_index(self, index_provider: DynamicIndexProvider) -> None:
        super().set_run_index(index_provider)
        self.script.set_run_index(index_provider)

    def execute(self, args: argparse.Namespace, fal_dbt: FalDbt) -> int:
        # Run the ephemeral model
        dbt_result = super().execute(args, fal_dbt)

        # And then run the Python script if it didn't fail.
        if dbt_result != SUCCESS:
            return dbt_result

        script_result = self.script.execute(args, fal_dbt)
        status = NodeStatus.Success if script_result == SUCCESS else NodeStatus.Error
        _mark_dbt_nodes_status_and_response(
            fal_dbt, status, self.script.bound_model.unique_id
        )
        return script_result


@dataclass
class FalLocalHookTask(Task):
    hook_path: Path
    bound_model: Optional[DbtModel] = None
    arguments: Optional[Dict[str, Any]] = None
    hook_type: HookType = HookType.HOOK

    @classmethod
    def from_fal_script(cls, script: FalScript):
        if (
            script.model is not None
            and script.model.python_model is not None
            and script.path == script.model.python_model
        ):
            hook_type = HookType.MODEL_SCRIPT
        else:
            hook_type = HookType.HOOK if script.is_hook else HookType.SCRIPT

        return cls(
            script.path,
            script.model,
            script.hook_arguments,
            hook_type,
        )

    def execute(self, args: argparse.Namespace, fal_dbt: FalDbt) -> int:
        script = self.build_fal_script(fal_dbt)
        return run_script(script, self.run_index)

    def build_fal_script(self, fal_dbt: FalDbt):
        if self.hook_type is HookType.MODEL_SCRIPT:
            return FalScript.model_script(fal_dbt, model=self.bound_model)
        else:
            return FalScript(
                fal_dbt,
                model=self.bound_model,
                path=str(self.hook_path),
                hook_arguments=self.arguments,
                is_hook=self.hook_type is HookType.HOOK,
            )


@dataclass
class FalIsolatedHookTask(Task):
    environment_name: str
    local_hook: FalLocalHookTask

    def set_run_index(self, index_provider: DynamicIndexProvider) -> None:
        super().set_run_index(index_provider)
        self.local_hook.set_run_index(index_provider)

    def execute(self, args: argparse.Namespace, fal_dbt: FalDbt) -> int:
        try:
            environment = fal_dbt._load_environment(self.environment_name)
        except:
            import traceback
            LOGGER.error("Could not find environment: {}\n{}", self.environment_name, traceback.format_exc())
            return FAILURE

        with environment.connect() as connection:
            execute_local_task = partial(
                self.local_hook.execute, args=args, fal_dbt=fal_dbt
            )
            result = connection.run(execute_local_task)
            assert isinstance(result, int), result
            return result

    @property
    def bound_model(self) -> DbtModel:
        return self.local_hook.bound_model


@dataclass
class TaskGroup:
    task: Task
    pre_hooks: List[Task] = field(default_factory=list)
    post_hooks: List[Task] = field(default_factory=list)
    dependencies: List[TaskGroup] = field(default_factory=list)
    status: Status = Status.PENDING

    def __post_init__(self):
        self._id = str(uuid.uuid4())

    @property
    def id(self) -> str:
        return self._id

    def iter_tasks(self) -> Iterator[Task]:
        yield from self.pre_hooks
        yield self.task
        yield from self.post_hooks
