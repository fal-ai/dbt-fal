from __future__ import annotations

import argparse
import sys
import traceback
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import cached_property

from dbt.logger import GLOBAL_LOGGER as logger
from fal.node_graph import FalScript
from faldbt.project import DbtModel, FalDbt

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
            use_temp_dirs=True,
        )
        return output.return_code


@dataclass
class FalModelTask(Task):
    model_name: str

    def execute(self, args: argparse.Namespace, fal_dbt: FalDbt) -> int:
        print(f"$ fal run {self.model_name}")
        print(f"(DONE) $ fal run {self.model_name}")
        return SUCCESS


@dataclass
class FalHookTask(Task):
    hook_path: str
    bound_model: Optional[DbtModel] = None

    def execute(self, args: argparse.Namespace, fal_dbt: FalDbt) -> int:
        script = FalScript(fal_dbt, self.bound_model, self.hook_path, True)
        logger.debug("Running script {}", script.id)
        try:
            with self._modify_path(fal_dbt):
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
    def _modify_path(self, fal_dbt: FalDbt):
        sys.path.append(fal_dbt.scripts_dir)
        yield
        sys.path.remove(fal_dbt.scripts_dir)


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
