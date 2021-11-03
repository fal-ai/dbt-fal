"""Run **ANYTHING** with FAL."""

import click
import os
import sys
from actions.actions import forecast, make_forecast
from faldbt.parse import parse_profile, parse_project


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

    filtered_models = list(
        filter(lambda model: model.meta.get(keyword, None) != None, project.models)
    )

    for model in filtered_models:
        for script in model.meta[keyword]['scripts']:
            real_script = os.path.join(dbt_dir, script)
            with open(real_script) as file:
                a_script = file.read()
                exec(a_script, {"ref": project.get_data_frame_for_model_name,
                                "current_model": model.name })
