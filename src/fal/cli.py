import click
import os

from dbt.logger import log_manager
from dbt.config.profile import DEFAULT_PROFILES_DIR

from fal.run_scripts import run_ordered_scripts, run_scripts
from faldbt.parse import parse_project
from fal.dag import ScriptGraph
from fal.utils import print_run_info


@click.group()
@click.version_option()
def cli():
    pass


@cli.command()
@click.option(
    "--project-dir",
    default=os.getcwd(),
    help="Directory to look for dbt_project.yml.",
    type=click.Path(exists=True),
)
@click.option(
    "--profiles-dir",
    default=None,
    help="Directory to look for profiles.yml.",
    type=click.Path(exists=True),
)
@click.option(
    "--keyword",
    default="fal",
    help="Property in meta to look for fal configurations.",
    type=click.STRING,
)
@click.option(
    "--all",
    help="Only run models that ran in the last dbt run.",
    is_flag=True,
)
@click.option(
    "--experimental-ordering",
    help="Turns on ordering of the fal scripts.",
    is_flag=True,
)
@click.option(
    "--debug",
    help="Display debug logging during execution.",
    is_flag=True,
)
def run(project_dir, profiles_dir, keyword, all, experimental_ordering, debug):
    with log_manager.applicationbound():
        if debug:
            log_manager.set_debug()

        real_project_dir = os.path.realpath(os.path.normpath(project_dir))
        real_profiles_dir = None
        if profiles_dir is not None:
            real_profiles_dir = os.path.realpath(os.path.normpath(profiles_dir))
        elif os.getenv("DBT_PROFILES_DIR"):
            real_profiles_dir = os.path.realpath(os.getenv("DBT_PROFILES_DIR"))
        else:
            real_profiles_dir = DEFAULT_PROFILES_DIR

        project = parse_project(real_project_dir, real_profiles_dir, keyword)
        models = project.get_filtered_models(all)
        manifest = project.manifest.nativeManifest
        print_run_info(models)

        if experimental_ordering:
            ordered_scripts = ScriptGraph(models, keyword, project_dir).sort()
            run_ordered_scripts(
                ordered_scripts, keyword, manifest, real_project_dir, real_profiles_dir
            )
        else:
            for model in models:
                run_scripts(
                    model,
                    keyword,
                    project.manifest.nativeManifest,
                    real_project_dir,
                    real_profiles_dir,
                )
