import os
import json
from typing import Dict, Any, List, Optional, Union, Callable
from pathlib import Path
from functools import partial
from dataclasses import dataclass, field
from deprecation import deprecated

from faldbt.parse import normalize_path
from faldbt.project import DbtModel, FalDbt

from dbt.contracts.results import RunStatus
from dbt.config.runtime import RuntimeConfig
from dbt.logger import GLOBAL_LOGGER as logger

from dbt.contracts.graph.parsed import ColumnInfo


class Hook:
    ...


@dataclass
class LocalHook(Hook):
    path: str
    arguments: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IsolatedHook(Hook):
    path: str
    environment: str
    arguments: Dict[str, Any] = field(default_factory=dict)


def create_hook(raw_hook: Any) -> Hook:
    if isinstance(raw_hook, str):
        return LocalHook(raw_hook)
    elif isinstance(raw_hook, dict):
        if "path" in raw_hook:
            if "environment" in raw_hook:
                return IsolatedHook(
                    raw_hook["path"],
                    raw_hook["environment"],
                    raw_hook.get("with", {}),
                )
            else:
                return LocalHook(raw_hook["path"], raw_hook.get("with", {}))
        else:
            raise ValueError(f"A hook must specify path.")

    raise ValueError(f"Unrecognized hook value: {raw_hook}")


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
class Context:
    current_model: Union[CurrentModel, None]
    config: ContextConfig
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

    def __init__(
        self,
        faldbt: FalDbt,
        model: Optional[DbtModel],
        path: str,
        hook_arguments: Optional[Dict[str, Any]] = None,
        is_hook: bool = False,
    ):
        # Necessary because of frozen=True
        object.__setattr__(self, "model", model)
        object.__setattr__(self, "path", normalize_path(faldbt.scripts_dir, path))
        object.__setattr__(self, "faldbt", faldbt)
        object.__setattr__(self, "hook_arguments", hook_arguments)
        object.__setattr__(self, "is_hook", is_hook)

    @classmethod
    def from_hook(cls, faldbt: FalDbt, model: DbtModel, hook: Hook):
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
        )

    @classmethod
    def model_script(cls, faldbt: FalDbt, model: DbtModel):
        script = FalScript(faldbt, model, "")
        # HACK: Set the script path specially for this case
        object.__setattr__(script, "path", model.python_model)
        return script

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
                "write_to_firestore": self.faldbt.write_to_firestore,
                "list_models": self.faldbt.list_models,
                "list_models_ids": self.faldbt.list_models_ids,
                "list_sources": self.faldbt.list_sources,
                "list_features": self.faldbt.list_features,
                "el": self.faldbt.el,
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
        context_config = ContextConfig(self.faldbt._config)
        if self.is_global:
            return Context(current_model=None, config=context_config)

        model: DbtModel = self.model  # type: ignore

        meta = model.meta or {}
        _del_key(meta, self.faldbt.keyword)

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
            adapter_response=current_adapter_response,
        )

        return Context(
            current_model=current_model,
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

    logger.debug(f"Joined .ipynb cells to:\n{joined_script}")

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
