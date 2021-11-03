"""Run **ANYTHING** with FAL."""

import click
import os
import sys
from actions.actions import forecast, make_forecast
from dbt.parse import parse_profile, parse_project


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
    for script in project.scripts:
        script_descriptor = open(script)
        a_script = script_descriptor.read()
        sys.argv = [project.get_data_frame]
        exec(a_script, {"ref": project.get_data_frame_for_model_name})
        script_descriptor.close()
