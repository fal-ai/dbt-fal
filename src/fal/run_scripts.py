"""Run fal scripts."""
from typing import Dict, Any, List
from dataclasses import dataclass

from dbt.contracts.results import NodeStatus
from dbt.logger import GLOBAL_LOGGER as logger

from faldbt.cp.contracts.graph.parsed import ColumnInfo
from faldbt.cp.contracts.graph.parsed import ColumnInfo
from faldbt.project import FalProject
from fal.dag import FalScript


@dataclass
class CurrentModel:
    name: str
    status: NodeStatus
    columns: Dict[str, ColumnInfo]
    meta: Dict[Any, Any]


@dataclass
class Context:
    current_model: CurrentModel


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
        context = Context(current_model=current_model)

        logger.info("Running script {} for model {}", script.path, model.name)

        script.exec(
            context,
            faldbt.ref,
            faldbt.source,
            faldbt.write_to_source,
            faldbt.write_to_firestore,
        )


def run_global_scripts(list: List[FalScript], project: FalProject):
    faldbt = project._faldbt
    for script in list:
        script.exec(
            None,
            faldbt.ref,
            faldbt.source,
            faldbt.write_to_source,
            faldbt.write_to_firestore,
        )


def _del_key(dict: Dict[str, Any], key: str):
    try:
        del dict[key]
    except KeyError:
        pass
