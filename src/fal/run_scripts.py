import os
from typing import Union, Dict, Any, List


from dbt.contracts.results import RunStatus, TestStatus, FreshnessStatus

from faldbt.cp.contracts.graph.parsed import ColumnInfo
from faldbt.project import FalProject, FalDbt, DbtModel

from dataclasses import dataclass
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


# TODO: deprecate this and use `run_orderd_scripts` only.
# Order the passed in list according to the flag.
def run_scripts(model: DbtModel, project: FalProject):
    faldbt: FalDbt = project._faldbt
    for script in model.meta.get(project.keyword, {}).get("scripts", []):
        meta = model.meta
        _del_key(meta, project.keyword)

        current_model = CurrentModel(
            name=model.name,
            status=project.get_model_status(model.name),
            columns=model.columns,
            meta=meta,
        )

        context = Context(current_model=current_model)
        real_script = os.path.join(faldbt.project_dir, script)
        with open(real_script) as file:
            a_script = file.read()
            exec(
                a_script,
                {
                    "context": context,
                    "ref": faldbt.ref,
                    "source": faldbt.source,
                    "write_to_source": faldbt.write_to_source,
                },
            )


def run_ordered_scripts(list: List[FalScript], project: FalProject):
    faldbt: FalDbt = project._faldbt
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

        script.exec(context, faldbt.ref, faldbt.source, faldbt.write_to_source)


def _del_key(dict: Dict[str, Any], key: str):
    try:
        del dict[key]
    except KeyError:
        pass
