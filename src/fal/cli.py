import click
import os

from dbt.logger import log_manager
from dbt.config.profile import DEFAULT_PROFILES_DIR

from fal.run_scripts import run_scripts
from faldbt.parse import parse_project
import faldbt.lib as lib


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
    help="Property in meta to look for fal configurations",
    type=click.STRING,
)
@click.option(
    "--all",
    is_flag=True,
    help="Only run models that ran in the last dbt run",
)
@click.option(
    "--debug",
    is_flag=True,
    help="Display debug logging during dbt execution",
)
def run(run, project_dir, profiles_dir, keyword, all, debug):
    with log_manager.applicationbound():
        if debug:
            log_manager.set_debug()

        real_project_dir = os.path.realpath(os.path.normpath(project_dir))
        real_profiles_dir = os.path.realpath(os.path.normpath(profiles_dir))

        project = parse_project(real_project_dir, real_profiles_dir, keyword)
        for model in project.get_filtered_models(all):
            run_scripts(
                model, keyword, project.manifest.nativeManifest, real_project_dir
            )
