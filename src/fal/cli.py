import click
import os

from dbt.logger import log_manager
from dbt.config.profile import DEFAULT_PROFILES_DIR

from fal.run_scripts import run_global_scripts, run_scripts
from fal.dag import FalScript, ScriptGraph
from fal.utils import print_run_info
from faldbt.project import FalDbt, FalGeneralException, FalProject


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
    "--select",
    "-s",
    multiple=True,
    default=tuple(),
    nargs=1,
    help="Specify the nodes to include.",
    type=click.STRING,
)
@click.option(
    "--models",
    "-m",
    nargs=1,
    multiple=True,
    default=tuple(),
    help="Specify the nodes to include.",
    type=click.STRING,
)
@click.option(
    "--exclude",
    nargs=1,
    multiple=True,
    default=tuple(),
    help="Specify the models to exclude.",
    type=click.STRING,
)
@click.option(
    "--selector",
    nargs=1,
    default=None,
    help="The selector name to use, as defined in selectors.yml",
    type=click.STRING,
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
def run(
    project_dir,
    profiles_dir,
    keyword,
    all,
    select,
    models,
    exclude,
    selector,
    experimental_ordering,
    debug,
):
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

        if not select and models:
            select = models

        selector_flags = select or exclude or selector
        if all and selector_flags:
            raise FalGeneralException(
                "Cannot pass --all flag alongside selection flags (--select/--models, --exclude, --selector)"
            )

        faldbt = FalDbt(
            real_project_dir, real_profiles_dir, select, exclude, selector, keyword
        )
        project = FalProject(faldbt)
        models = project.get_filtered_models(all, selector_flags)
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
