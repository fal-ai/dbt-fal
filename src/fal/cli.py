import click
import os

from dbt.logger import log_manager
from dbt.config.profile import DEFAULT_PROFILES_DIR

from fal.run_scripts import run_global_scripts, run_scripts
from fal.dag import FalScript, ScriptGraph
from fal.utils import print_run_info
from faldbt.project import FalDbt, FalProject


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
    help="Run scripts for all models. By default, fal runs scripts for models that ran in the last dbt run.",
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
@click.option(
    "--model_name",
    "-m",
    default=None,
    help="[Optional] Name of a single model to run a script on. Disregards model YAML definitions. Requires --script_path.",
    type=click.STRING,
)
@click.option(
    "--script_path",
    "-s",
    default=None,
    help="[Optional] Name of a single script to run on a specific model. Disregards model YAML definitions. Requires --model_name.",
    type=click.STRING,
)
def run(project_dir, profiles_dir, keyword, all, experimental_ordering, debug, model_name, script_path):
    with log_manager.applicationbound():
        if debug:
            log_manager.set_debug()

        real_project_dir = os.path.realpath(os.path.normpath(project_dir))
        real_profiles_dir = None
        if profiles_dir is not None:
            real_profiles_dir = os.path.realpath(os.path.normpath(profiles_dir))
        elif os.getenv("DBT_PROFILES_DIR"):
            real_profiles_dir = os.path.realpath(
                os.path.normpath(os.getenv("DBT_PROFILES_DIR"))
            )
        else:
            real_profiles_dir = DEFAULT_PROFILES_DIR

        faldbt = FalDbt(real_project_dir, real_profiles_dir, keyword)
        project = FalProject(faldbt)
        if model_name is not None:
            if script_path is None:
                raise Exception("--model_name (-m) requires --script_path (-s)")
            models = project.get_all_models()
            model = next(x for x in models if x.name == model_name)
            run_scripts([FalScript(model, script_path)], project)
        else:

            models = project.get_filtered_models(all)
            print_run_info(models, keyword)

            if experimental_ordering:
                scripts = ScriptGraph(models, keyword, project_dir).sort()
            else:
                scripts = []
                for model in models:
                    for path in model.get_script_paths(keyword, real_project_dir):
                        scripts.append(FalScript(model, path))

            # run model specific scripts first
            run_scripts(scripts, project)

            # then run global scripts
            global_scripts = list(
                map(
                    lambda path: FalScript(None, path, []),
                    faldbt._global_script_paths,
                )
            )

            run_global_scripts(global_scripts, project)
