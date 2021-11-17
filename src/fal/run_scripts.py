import os
from typing import Optional, Union, Dict, Any

import faldbt.lib as lib

from dbt.contracts.graph.manifest import Manifest, MaybeNonSource, MaybeParsedSource
from dbt.contracts.graph.parsed import ParsedModelNode
from dbt.contracts.results import RunStatus, TestStatus, FreshnessStatus

from faldbt.cp.contracts.graph.parsed import ColumnInfo
from faldbt.project import DbtModel

import pandas as pd
from dataclasses import dataclass


@dataclass
class CurrentModel:
    name: str
    status: Union[RunStatus, TestStatus, FreshnessStatus]
    columns: Dict[str, ColumnInfo]
    meta: Dict[Any, Any]


@dataclass
class Context:
    current_model: CurrentModel


def run_scripts(model: DbtModel, keyword: str, manifest: Manifest, dbt_dir: str):
    for script in model.meta.get(keyword, {}).get("scripts", []):
        meta = model.meta
        _del_key(meta, keyword)

        current_model = CurrentModel(
            name=model.name, status=model.status, columns=model.columns, meta=meta
        )

        context = Context(current_model=current_model)
        real_script = os.path.join(dbt_dir, script)
        with open(real_script) as file:
            a_script = file.read()
            exec(
                a_script,
                {
                    "ref": _get_ref_resolver(model.node, manifest, dbt_dir),
                    "context": context,
                    "source": _get_source_resolver(model.node, manifest, dbt_dir),
                    "write_to_source": _write_to_source(model.node, manifest, dbt_dir),
                },
            )


def _del_key(dict: Dict[str, Any], key: str):
    try:
        del dict[key]
    except KeyError:
        pass


def _get_ref_resolver(
    model: ParsedModelNode,
    manifest: Manifest,
    dbt_dir: str,
):
    def ref_resolver(target_model_name: str, target_package_name: Optional[str] = None):
        target_model: MaybeNonSource = manifest.resolve_ref(
            target_model_name, target_package_name, dbt_dir, model.package_name
        )
        if target_model is None:
            raise Exception(
                f"Could not find model {target_model_name}{target_package_name or ''}"
            )

        result = lib.fetch_target(manifest, dbt_dir, target_model)
        return pd.DataFrame.from_records(
            result.table.rows, columns=result.table.column_names
        )

    return ref_resolver


def _get_source_resolver(
    model: ParsedModelNode,
    manifest: Manifest,
    dbt_dir: str,
):
    def source_resolver(target_source_name: str, target_table_name: str):
        target_source: MaybeParsedSource = manifest.resolve_source(
            target_source_name, target_table_name, dbt_dir, model.package_name
        )

        if target_source is None:
            raise Exception(
                f"Could not find source {target_source_name}.{target_table_name}"
            )
        result = lib.fetch_target(manifest, dbt_dir, target_source)
        return pd.DataFrame.from_records(
            result.table.rows, columns=result.table.column_names
        )

    return source_resolver


def _write_to_source(
    model: ParsedModelNode,
    manifest: Manifest,
    dbt_dir: str,
):
    def source_writer(
        data: pd.DataFrame, target_source_name: str, target_table_name: str
    ):
        target_source: MaybeParsedSource = manifest.resolve_source(
            target_source_name, target_table_name, dbt_dir, model.package_name
        )

        if target_source is None:
            raise Exception(
                f"Could not find source '{target_source_name}'.'{target_table_name}'"
            )

        lib.write_target(data, manifest, dbt_dir, target_source)

    return source_writer
