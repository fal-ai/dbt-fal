from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any, Dict

from dbt.adapters.base import BaseAdapter, PythonJobHelper
from dbt.adapters.python.impl import PythonAdapter
from dbt.contracts.connection import AdapterResponse
from fal.planner.tasks import (
    FAILURE,
    SUCCESS,
    FalIsolatedHookTask,
    FalLocalHookTask,
    HookType,
    Task,
)

from .connections import FalConnectionManager, FalCredentials

if TYPE_CHECKING:
    from fal.planner.tasks import Task
    from faldbt.project import FalDbt


@dataclass
class DBTModelReference:
    """Reference for a DBT Python model."""

    unique_id: str


@dataclass
class StaticRunIndex:
    run_index: int = 0

    def next(self):
        return self.run_index


class FalExecutionHelper(PythonJobHelper):
    def __init__(
        self, parsed_model: Dict[str, Any], credentials: FalCredentials
    ) -> None:
        self.parsed_model = parsed_model
        self.credentials = credentials

        # fal_environment specifies the environment in which the
        # given model should be run in. All environments must be
        # initially defined inside fal_project.yml.
        self.environment = parsed_model["config"].get("fal_environment", "local")
        self.model_id = parsed_model["unique_id"]
        self.index_provider = StaticRunIndex()

    def _create_fal_task(self, code_path: Path) -> Task:
        task = FalLocalHookTask(
            code_path,
            bound_model=DBTModelReference(self.model_id),
            hook_type=HookType.MODEL_SCRIPT,
        )

        if self.environment != "local":
            # If the model needs to run in an isolated environment,
            # we'll mark it as such.
            task = FalIsolatedHookTask(self.environment, local_hook=task)

        task.set_run_index(self.index_provider)
        return task

    def _execute_fal_task(self, task: Task) -> int:
        fal_dbt = self.credentials.to_fal_dbt()
        return task.execute(None, fal_dbt)

    def submit(self, compiled_code: str) -> int:
        """Execute the given `compiled_code` in the target environment
        via Fal."""

        # We are going to save the generated code in a temporary file
        # on the disk and then execuute it as if we are exeucting a
        # Fal Python model (by constructing a task object).
        with NamedTemporaryFile(mode="w+t") as tmp_file:
            tmp_file.write(compiled_code)
            tmp_file.flush()

            task = self._create_fal_task(Path(tmp_file.name))
            return self._execute_fal_task(task)


class FalAdapter(PythonAdapter):
    ConnectionManager = FalConnectionManager

    def generate_python_submission_response(
        self, submission_result: int
    ) -> AdapterResponse:
        if submission_result == SUCCESS:
            return AdapterResponse("SUCCESS")
        else:
            return AdapterResponse("FAILURE")

    @property
    def default_python_submission_method(self) -> str:
        return "fal"

    @property
    def python_submission_helpers(self) -> Dict[str, PythonJobHelper]:
        return {
            "fal": FalExecutionHelper,
        }

    @classmethod
    def type(cls):
        return "fal"

    @classmethod
    def is_cancelable(cls) -> bool:
        return False
