import os
import json
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
from functools import partial
from dataclasses import dataclass
from deprecation import deprecated

from faldbt.parse import normalize_path
from faldbt.project import DbtModel, FalDbt
import faldbt.lib as lib

from dbt.contracts.results import RunStatus
from dbt.config.runtime import RuntimeConfig
from dbt.logger import GLOBAL_LOGGER as logger

if lib.DBT_VCURRENT.compare(lib.DBT_V1) >= 0:
    from dbt.contracts.graph.parsed import ColumnInfo
else:
    from faldbt.cp.contracts.graph.parsed import ColumnInfo


@dataclass
class CurrentModel:
    name: str
    status: RunStatus
    columns: Dict[str, ColumnInfo]
    tests: List[Any]
    meta: Dict[Any, Any]


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


@dataclass(frozen=True, init=False)
class FalScript:
    model: Optional[DbtModel]
    path: Path
    _faldbt: FalDbt

    def __init__(self, faldbt: FalDbt, model: Optional[DbtModel], path: str):
        # Necessary because of frozen=True
        object.__setattr__(self, "model", model)
        object.__setattr__(self, "path", normalize_path(faldbt.scripts_dir, path))
        object.__setattr__(self, "_faldbt", faldbt)

    def exec(self, faldbt: FalDbt):
        """
        Executes the script
        """
        # Enable local imports
        try:
            if str(self.path).endswith(".ipynb"):
                raw_source_code = _process_ipynb(str(self.path))
                source_code = compile(raw_source_code, self.path, "exec")
            else:
                with open(self.path) as file:
                    source_code = compile(file.read(), self.path, "exec")

            exec_globals = {
                "context": self._build_script_context(),
                "ref": faldbt.ref,
                "source": faldbt.source,
                "write_to_source": faldbt.write_to_source,
                "write_to_firestore": faldbt.write_to_firestore,
                "list_models": faldbt.list_models,
                "list_models_ids": faldbt.list_models_ids,
                "list_sources": faldbt.list_sources,
                "list_features": faldbt.list_features,
                "el": faldbt.el,
            }

            if self.model is not None:
                # Hard-wire the model
                exec_globals["write_to_model"] = partial(
                    faldbt.write_to_model, target_1=self.model.name, target_2=None
                )

            exec(source_code, exec_globals)
        finally:
            pass

    @property
    def id(self):
        # TODO: maybe `self.path - project_dir`, to show only relevant path
        return f"({self.model_name},{self.path})"

    @property
    def is_global(self):
        return self.model is None

    @property
    def model_name(self):
        return "<GLOBAL>" if self.is_global else self.model.name  # type: ignore

    def _build_script_context(self):
        context_config = ContextConfig(self._faldbt._config)
        if self.is_global:
            return Context(current_model=None, config=context_config)

        model: DbtModel = self.model  # type: ignore

        meta = model.meta
        _del_key(meta, self._faldbt.keyword)

        tests = _process_tests(model.tests)

        current_model = CurrentModel(
            name=model.name,
            status=model.status,
            columns=model.columns,
            tests=tests,
            meta=meta,
        )

        return Context(current_model=current_model, config=context_config)


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


def _process_ipynb(filepath: str) -> str:
    def strip_magic(source: List[str]) -> List[str]:
        IMPORT_STMT = "from faldbt.magics import init_fal"
        return (item for item in source if item[0] != "%" and item != IMPORT_STMT)

    with open(filepath) as raw_data:
        raw_script = json.load(raw_data)

    script_list = []
    for cell in raw_script["cells"]:
        if cell["cell_type"] == "code":
            source = strip_magic(cell["source"])
            script_list.append("".join(source))

    joined_script = "\n #cell \n".join(script_list)

    logger.debug(f"Joined .ipynb cells to:\n{joined_script}")

    return joined_script
