import os
import json
from enum import Enum
from typing import Dict, Any, List, Optional, Union, Callable
from pathlib import Path
from functools import partial
from dataclasses import dataclass, field
from deprecation import deprecated

import hashlib

from fal.dbt.integration.parse import normalize_path
from fal.dbt.integration.project import DbtModel, FalDbt, FAL

from dbt.contracts.results import RunStatus
from dbt.config.runtime import RuntimeConfig
from fal.dbt.integration.logger import LOGGER

from fal.dbt.telemetry import telemetry

from dbt.contracts.graph.nodes import ColumnInfo


class TimingType(Enum):
    PRE = "pre"
    POST = "post"

    def for_script(self):
        if self == TimingType.PRE:
            return "before"
        elif self == TimingType.POST:
            return "after"
        else:
            raise ValueError(f"Unknown timing type: {self}")

    def for_hook(self):
        return self.value

    def __str__(self):
        return self.for_hook()


class Hook:
    path: str
    arguments: Dict[str, Any]


@dataclass
class LocalHook(Hook):
    path: str
    arguments: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IsolatedHook(Hook):
    path: str
    environment_name: str
    arguments: Dict[str, Any] = field(default_factory=dict)


def _is_local_environment(environment_name: str) -> None:
    return environment_name == "local"


def create_hook(raw_hook: Any, default_environment_name: Optional[str] = None) -> Hook:
    if isinstance(raw_hook, str):
        raw_hook = {"path": raw_hook}

    if not isinstance(raw_hook, dict):
        raise ValueError(f"Unrecognized hook value: {raw_hook}")

    if "path" not in raw_hook:
        raise ValueError(f"A hook must specify path.")

    environment_name = raw_hook.get("environment", default_environment_name)
    if environment_name and not _is_local_environment(environment_name):
        return IsolatedHook(
            raw_hook["path"],
            environment_name,
            raw_hook.get("with", {}),
        )
    else:
        return LocalHook(raw_hook["path"], raw_hook.get("with", {}))


@dataclass
class CurrentAdapterResponse:
    message: str
    code: Optional[str]
    rows_affected: Optional[int]


@dataclass
class CurrentModel:
    name: str
    alias: str
    status: RunStatus
    columns: Dict[str, ColumnInfo]
    tests: List[Any]
    meta: Dict[Any, Any]
    is_incremental: str
    adapter_response: Optional[CurrentAdapterResponse]


@dataclass
class CurrentTest:
    name: str
    model_name: str
    column: str
    status: str

    @property
    @deprecated(details="Use 'model_name' instead")
    def modelname(self):
        return self.model_name


@dataclass
class ContextConfig:
    target_path: Path

    def __init__(self, config: RuntimeConfig):
        self.target_path = Path(
            os.path.realpath(os.path.join(config.project_root, config.target_path))
        )


@dataclass
class ContextTarget:
    def __init__(self, config: RuntimeConfig):
        self.profile_name = config.profile_name
        self.name = config.target_name
        self.threads = config.threads
        self.type = config.credentials.type
        self.database = config.credentials.database
        self.schema = config.credentials.schema


@dataclass
class Context:
    current_model: Union[CurrentModel, None]
    config: ContextConfig
    target: ContextTarget
    _arguments: Optional[Dict[str, Any]] = field(repr=False, default=None)

    @property
    def arguments(self) -> Dict[str, Any]:
        if self._arguments is None:
            raise ValueError(
                "'context.arguments' is only accessible from hooks, "
                "not from scripts/models"
            )
        return self._arguments


@dataclass(frozen=True, init=False)
class FalScript:
    model: Optional[DbtModel]
    path: Path
    faldbt: FalDbt
    hook_arguments: Optional[Dict[str, Any]]
    is_hook: bool
    timing_type: Optional[TimingType]

    def __init__(
        self,
        faldbt: FalDbt,
        model: Optional[DbtModel],
        path: Union[str, Path],
        hook_arguments: Optional[Dict[str, Any]] = None,
        is_hook: bool = False,
        is_model: bool = False,
        timing_type: Optional[TimingType] = None,
    ):
        # Necessary because of frozen=True
        object.__setattr__(self, "model", model)
        object.__setattr__(
            self, "path", path if is_model else normalize_path(faldbt.scripts_dir, path)
        )
        object.__setattr__(self, "faldbt", faldbt)
        object.__setattr__(self, "hook_arguments", hook_arguments)
        object.__setattr__(self, "is_hook", is_hook)
        object.__setattr__(self, "timing_type", timing_type)

        self._telemetry()

    def _telemetry(self):
        try:
            _is_global = self.model is None
            _is_hook = self.is_hook
            _timing_type = str(self.timing_type) if self.timing_type else None
            _is_model = self.model and self.model.python_model == self.path
            _path_hash = hashlib.md5(str(self.path).encode()).hexdigest()

            _script_timing_desc = (
                self.timing_type.for_script() if self.timing_type else "error"
            )
            _hook_timing_desc = (
                self.timing_type.for_hook() if self.timing_type else "error"
            )

            if _is_global:
                _script_desc = f"{_script_timing_desc}-global"
                # globals are not hooks nor scripts
                _is_hook = None

            elif _is_model:
                _script_desc = "fal-model"
                _is_global = None
                # models are not hooks nor scripts
                _is_hook = None

            else:
                if _is_hook:
                    _script_desc = f"{_hook_timing_desc}-hook"
                else:
                    _script_desc = f"{_script_timing_desc}-script"

            telemetry.log_api(
                action="falscript_initialized",
                additional_props={
                    "is_global": _is_global,
                    "is_hook": _is_hook,
                    "is_model": _is_model,
                    "script_timing_type": _timing_type,
                    "script_desc": _script_desc,
                    "script_path": _path_hash,
                },
            )
        except:
            # Ignore telemetry errors
            pass

    @classmethod
    def from_hook(
        cls, faldbt: FalDbt, model: DbtModel, hook: Hook, timing_type: TimingType
    ):
        """
        Creates a FalScript from a hook
        """
        assert isinstance(hook, LocalHook)
        return cls(
            faldbt=faldbt,
            model=model,
            path=hook.path,
            hook_arguments=hook.arguments,
            is_hook=True,
            timing_type=timing_type,
        )

    @classmethod
    def model_script(cls, faldbt: FalDbt, model: DbtModel):
        assert model.python_model, "path for Python models must be set"
        return FalScript(
            faldbt=faldbt, model=model, path=model.python_model, is_model=True
        )

    def exec(self):
        """
        Executes the script
        """
        # Enable local imports
        try:
            source_code = python_from_file(self.path)
            program = compile(source_code, self.path, "exec")

            exec_globals = {
                "__name__": "__main__",
                "context": self._build_script_context(),
                "ref": self.faldbt.ref,
                "source": self.faldbt.source,
                "list_models": self.faldbt.list_models,
                "list_models_ids": self.faldbt.list_models_ids,
                "list_sources": self.faldbt.list_sources,
                "list_features": self.faldbt.list_features,
                "execute_sql": self.faldbt.execute_sql,
            }

            if not self.is_hook:
                exec_globals["write_to_source"] = self.faldbt.write_to_source

                if self.model is not None:
                    # Hard-wire the model
                    exec_globals["write_to_model"] = partial(
                        self.faldbt.write_to_model,
                        target_1=self.model.name,
                        target_2=None,
                    )

            else:
                exec_globals["write_to_source"] = _not_allowed_function_maker(
                    "write_to_source"
                )
                exec_globals["write_to_model"] = _not_allowed_function_maker(
                    "write_to_model"
                )
            exec(program, exec_globals)
        finally:
            pass

    @property
    def relative_path(self):
        if self.is_model:
            return self.path.relative_to(self.faldbt.project_dir)
        else:
            return self.path.relative_to(self.faldbt.scripts_dir)

    @property
    def id(self):
        if self.is_model:
            return f"(model: {self.relative_path})"
        else:
            return f"({self.model_name}, {self.relative_path})"

    @property
    def is_global(self):
        return self.model is None

    @property
    def is_model(self):
        if self.model is not None and self.model.python_model is not None:
            return self.model.python_model == self.path

    @property
    def model_name(self):
        return "<GLOBAL>" if self.is_global else self.model.name  # type: ignore

    def _build_script_context(self) -> Context:
        config: RuntimeConfig = self.faldbt._config
        context_config = ContextConfig(config)
        target = ContextTarget(config)

        if self.is_global:
            return Context(current_model=None, target=target, config=context_config)

        model: DbtModel = self.model  # type: ignore

        meta = model.meta or {}
        _del_key(meta, FAL)

        tests = _process_tests(model.tests)

        current_adapter_response = None
        if model.adapter_response:
            current_adapter_response = CurrentAdapterResponse(
                message=str(model.adapter_response),
                code=model.adapter_response.code,
                rows_affected=model.adapter_response.rows_affected,
            )

        current_model = CurrentModel(
            name=model.name,
            alias=model.alias,
            status=model.status,
            columns=model.columns,
            tests=tests,
            meta=meta,
            is_incremental=model.is_incremental,
            adapter_response=current_adapter_response,
        )

        return Context(
            current_model=current_model,
            target=target,
            config=context_config,
            _arguments=self.hook_arguments,
        )


def _del_key(dict: Dict[str, Any], key: str):
    try:
        del dict[key]
    except KeyError:
        pass


def _process_tests(tests: List[Any]):
    return list(
        map(
            lambda test: CurrentTest(
                name=test.name,
                column=test.column,
                status=test.status,
                model_name=test.model,
            ),
            tests,
        )
    )


def python_from_file(path: Path) -> str:
    with open(path) as file:
        raw_source_code = file.read()
        if path.suffix == ".ipynb":
            raw_source_code = _process_ipynb(raw_source_code)
    return raw_source_code


def _process_ipynb(raw_source_code: str) -> str:
    def strip_magic(source: List[str]) -> List[str]:
        NOTEBOOK_LIB = "faldbt.magics"
        return [item for item in source if item[0] != "%" and NOTEBOOK_LIB not in item]

    ipynb_struct = json.loads(raw_source_code)

    script_list = []
    for cell in ipynb_struct["cells"]:
        if cell["cell_type"] == "code":
            source = strip_magic(cell["source"])
            script_list.append("".join(source))

    joined_script = "\n #cell \n".join(script_list)

    LOGGER.debug(f"Joined .ipynb cells to:\n{joined_script}")

    return joined_script


def _not_allowed_function_maker(function_name: str) -> Callable[[Any], None]:
    def not_allowed_function(*args, **kwargs):
        raise Exception(
            (
                f"{function_name} is not allowed in hooks."
                " Consider using a Python model."
            )
        )

    return not_allowed_function
