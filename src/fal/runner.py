"""Run **ANYTHING** with FAL."""

import click
import os
import sys
from actions.actions import forecast, make_forecast
from dbt.parse import parse_project
from typing import Dict, Any


@click.command()
@click.argument("run")
@click.option(
    "--dbt-dir",
    default=os.getcwd(),
    help="Directory to look for dbt_project.yml",
    type=click.Path(exists=True),
)
@click.option(
    "--keyword",
    default="fal",
    help="This keyword is used if we need to parse meta",
    type=click.STRING,
)
def run(run, dbt_dir, keyword):
    project = parse_project(dbt_dir, keyword)

    changed_model_names = list(
        map(lambda result: result.unique_id.split(".")[-1], project.results.results)
    )

    filtered_models = list(
        filter(
            lambda model: (model.meta.get(keyword, None) != None)
            and model.name in changed_model_names,
            project.models,
        )
    )

    for model in filtered_models:
        for script in model.meta[keyword]["scripts"]:
            ## remove scripts put everything else as args
            args = model.meta[keyword]
            _del_key(args, "scripts")
            args.update({"current_model": model.name})

            real_script = os.path.join(dbt_dir, script)
            with open(real_script) as file:
                a_script = file.read()
                exec(
                    a_script,
                    {
                        "ref": project.get_data_frame_for_model_name,
                        "args": args,
                    },
                )


def _del_key(dict: Dict[str, Any], key: str):
    try:
        del dict[key]
    except KeyError:
        pass
