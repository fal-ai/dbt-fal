"""Run **ANYTHING** with FAL."""

from typing import List, Optional, Union
import click
import os
import sys
from dbt.config import project

from dbt.context.providers import RuntimeRefResolver
from dbt.contracts.graph.compiled import CompiledModelNode, ManifestNode
from dbt.contracts.graph.parsed import ParsedModelNode
from dbt.node_types import NodeType
from actions.actions import forecast, make_forecast
from faldbt.parse import parse_project
from typing import Dict, Any
import faldbt.lib as lib
import json
import dbt.tracking
from dbt.config.profile import DEFAULT_PROFILES_DIR

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
    "--changed-only",
    is_flag=True,
    help="To only run models that ran in the last dbt run",
)
def run(run, dbt_dir, profiles_dir, keyword, changed_only):
    config = lib.get_dbt_config(dbt_dir)

    dbt.tracking.initialize_tracking(profiles_dir) # Necessary for parse_to_manifest to not fail
    manifest = lib.parse_to_manifest(config)

    result = lib.execute_sql(manifest, dbt_dir, 'SELECT * FROM `learning-project-305919`.`dbt_matteo`.`cut_times__latest_timestamp`')
    print(result.table.column_names)
    for row in result.table.rows:
        print(row)

    project = parse_project(dbt_dir, keyword)

    changed_model_names = list(
        map(lambda result: result.unique_id.split(".")[-1], project.results.results)
    )

    filtered_models: List[ParsedModelNode] = []
    for node in manifest.nodes.values():
        if node.resource_type == NodeType.Model:
            if changed_only and node.name not in changed_model_names:
                continue
            if keyword in node.config.meta:
                filtered_models.append(node)

    for model in filtered_models:
        def ref_resolver(target_model_name: str, target_package_name: Optional[str] = None):
            target_model = manifest.resolve_ref(target_model_name, target_package_name, dbt_dir, model.package_name)
            return lib.fetch_model(manifest, dbt_dir, target_model)

        for script in model.config.meta.get(keyword, {}).get('scripts', []):
            ## remove scripts put everything else as args
            args = model.config.meta[keyword]
            _del_key(args, "scripts")
            args.update({"current_model": model.name})

            real_script = os.path.join(dbt_dir, script)
            with open(real_script) as file:
                a_script = file.read()
                exec(a_script, { "ref": ref_resolver, "args": args })


def _del_key(dict: Dict[str, Any], key: str):
    try:
        del dict[key]
    except KeyError:
        pass
