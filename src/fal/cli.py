import click
import os

from fal.run_scripts import run_scripts
from faldbt.parse import parse_project
from dbt.config.profile import DEFAULT_PROFILES_DIR


@click.command()
@click.argument("run")
@click.option(
    "--project-dir",
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
def run(run, project_dir, profiles_dir, keyword, all):
    real_project_dir = os.path.realpath(os.path.normpath(project_dir))
    real_profiles_dir = os.path.realpath(os.path.normpath(profiles_dir))

    project = parse_project(real_project_dir, real_profiles_dir, keyword)
    for model in project.get_filtered_models(all):
        run_scripts(model, keyword, project.manifest.nativeManifest, real_project_dir)
