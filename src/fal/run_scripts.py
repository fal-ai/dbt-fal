"""Run fal scripts."""
import os
from typing import Dict, Any, List, Union
from dataclasses import dataclass
from dbt.config.runtime import RuntimeConfig
from pathlib import Path

from dbt.contracts.results import NodeStatus
from dbt.logger import GLOBAL_LOGGER as logger

from faldbt.project import FalProject
from fal.dag import FalScript

import faldbt.lib as lib

if lib.DBT_VCURRENT.compare(lib.DBT_V1) >= 0:
    from dbt.contracts.graph.parsed import ColumnInfo
else:
    from faldbt.cp.contracts.graph.parsed import ColumnInfo


@dataclass
class CurrentModel:
    name: str
    status: NodeStatus
    columns: Dict[str, ColumnInfo]
    meta: Dict[Any, Any]


@dataclass
class ContextConfig:
    target_path: Path


@dataclass
class Context:
    current_model: Union[CurrentModel, None]
    config: ContextConfig


def run_scripts(list: List[FalScript], project: FalProject):
    faldbt = project._faldbt
    for script in list:
        model = script.model
        meta = model.meta
        _del_key(meta, project.keyword)

        current_model = CurrentModel(
            name=model.name,
            status=project.get_model_status(model),
            columns=model.columns,
            meta=meta,
        )

        context_config = ContextConfig(_get_target_path(faldbt._config))
        context = Context(current_model=current_model, config=context_config)

        logger.info("Running script {} for model {}", script.path, model.name)

        script.exec(context, faldbt)


def run_global_scripts(list: List[FalScript], project: FalProject):
    faldbt = project._faldbt
    for script in list:
        context_config = ContextConfig(_get_target_path(faldbt._config))
        context = Context(current_model=None, config=context_config)

        logger.info("Running global script {}", script.path)

        script.exec(context, faldbt)


def _del_key(dict: Dict[str, Any], key: str):
    try:
        del dict[key]
    except KeyError:
        pass


def _get_target_path(config: RuntimeConfig) -> Path:
    return Path(os.path.realpath(os.path.join(config.project_root, config.target_path)))
