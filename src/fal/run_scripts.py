"""Run fal scripts."""
from typing import Union, Dict, Any, List
from dataclasses import dataclass

from dbt.contracts.results import RunStatus, TestStatus, FreshnessStatus

from faldbt.cp.contracts.graph.parsed import ColumnInfo
from faldbt.cp.contracts.graph.parsed import ColumnInfo
from faldbt.project import FalProject
from fal.dag import FalScript


@dataclass
class CurrentModel:
    name: str
    status: Union[RunStatus, TestStatus, FreshnessStatus]
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
            status=project.get_model_status(model.name),
            columns=model.columns,
            meta=meta,
        )
        context = Context(current_model=current_model)

        script.exec(
            context,
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
