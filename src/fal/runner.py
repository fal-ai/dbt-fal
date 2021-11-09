"""Run **ANYTHING** with FAL."""

from typing import List, Optional, Union
import click
import os
import sys
from dbt.config import project
from dbt.contracts.graph.manifest import Manifest

from dbt.contracts.graph.parsed import ParsedModelNode
from dbt.node_types import NodeType
from faldbt.parse import parse_project
from typing import Dict, Any
import faldbt.lib as lib
from dbt.config.profile import DEFAULT_PROFILES_DIR

import pandas as pd


@click.command()
@click.argument("run")
@click.option(
    "--dbt-dir",
    default=os.getcwd(),
    help="Directory to look for dbt_project.yml",
    type=click.Path(exists=True),
)
@click.option(
    "--profiles-dir",
    default=DEFAULT_PROFILES_DIR,
    help="Directory to look for profiles.yml",
    type=click.Path(exists=True),
)
@click.option(
    "--keyword",
    default="fal",
    help="This keyword is used if we need to parse meta",
    type=click.STRING,
)
@click.option(
    "--all",
    is_flag=True,
    help="To only run models that ran in the last dbt run",
)
def run(run, dbt_dir, profiles_dir, keyword, all):
    project = parse_project(dbt_dir, profiles_dir, keyword)

    manifest = project.manifest.nativeManifest

    for model in project.get_filtered_models(all):

        def ref_resolver(
            target_model_name: str, target_package_name: Optional[str] = None
        ):
            target_model = manifest.resolve_ref(
                target_model_name, target_package_name, dbt_dir, model.package_name
            )
            result = lib.fetch_model(manifest, dbt_dir, target_model)
            return pd.DataFrame.from_records(
                result.table.rows, columns=result.table.column_names
            )

        def source_resolver(target_source_name: str, target_table_name: str):
            target_source = manifest.resolve_source(
                target_source_name, target_table_name, dbt_dir, model.package_name
            )
            result = lib.fetch_model(manifest, dbt_dir, target_source)
            return pd.DataFrame.from_records(
                result.table.rows, columns=result.table.column_names
            )

        for script in model.config.meta.get(keyword, {}).get("scripts", []):
            ## remove scripts put everything else as context
            meta = model.config.meta[keyword]
            _del_key(meta, "scripts")
            current_model = {
                "name": model.name,
                "status": None,  # TODO: get status from run status
            }
            context = {"meta": meta, "current_model": current_model}
            real_script = os.path.join(dbt_dir, script)
            with open(real_script) as file:
                a_script = file.read()
                exec(
                    a_script,
                    {
                        "ref": ref_resolver,
                        "context": context,
                        "source": source_resolver,
                    },
                )


def _del_key(dict: Dict[str, Any], key: str):
    try:
        del dict[key]
    except KeyError:
        pass
