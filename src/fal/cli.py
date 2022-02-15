import argparse
import os
import sys
import pkg_resources

from dbt.logger import log_manager
from dbt.config.profile import DEFAULT_PROFILES_DIR

from fal.run_scripts import run_global_scripts, run_scripts
from fal.dag import FalScript, ScriptGraph
from fal.utils import print_run_info
from faldbt.project import FalDbt, FalGeneralException, FalProject


class FalCli(object):

    def __init__(self, argv):
        parser = argparse.ArgumentParser(
            description="Run Python scripts on dbt models",
            usage='''fal run [<args>]''')

        # Handle version checking
        version = pkg_resources.get_distribution('fal').version
        parser.add_argument("-v",
                            "--version",
                            action="version",
                            version=f"fal {version}",
                            help="show fal version")

        # Handle commands
        parser.add_argument("command", help="Subcommand to run")

        args = parser.parse_args(argv[1:2])
        if not hasattr(self, args.command):
            print("Unrecognized command")
            parser.print_help()
            exit(1)

        getattr(self, args.command)(argv)


    def run(self, argv):
        parser = argparse.ArgumentParser(description="Run Python scripts as final nodes")
        parser.add_argument("--project-dir",
                            default=os.getcwd(),
                            help="Directory to look for dbt_project.yml.")
        parser.add_argument("--profiles-dir",
                            default=None,
                            help="Directory to look for profiles.yml.")
        parser.add_argument("--keyword",
                            default="fal",
                            help="Property in meta to look for fal configurations.")
        parser.add_argument("--all",
                            default=False,
                            action='store_true',
                            help="Run scripts for all models. By default, fal runs scripts for models that ran in the last dbt run.")
        parser.add_argument("-s", "--select",
                            default=tuple(),
                            nargs="+",
                            help="Specify the nodes to include.")
        parser.add_argument("-m", "--models",
                            default=tuple(),
                            nargs="+",
                            help="Specify the nodes to include.")
        parser.add_argument("--exclude",
                            default=tuple(),
                            nargs="+",
                            help="Specify the nodes to exclude.")
        parser.add_argument("--selector",
                            default=None,
                            action='store_true',
                            help="The selector name to use, as defined in selectors.yml",)
        parser.add_argument("--scripts",
                            default=None,
                            nargs="+",
                            help="Specify scripts to run, overrides schema.yml",)
        parser.add_argument("--before",
                            action='store_true',
                            help="Run scripts specified in model `before` tag",
                            default=False)
        parser.add_argument("--experimental-ordering",
                            action='store_true',
                            help="Turns on ordering of the fal scripts.",
                            default=False)
        parser.add_argument("--debug",
                            action='store_true',
                            help="Display debug logging during execution.",
                            default=False)

        args = parser.parse_args(argv[2:])

        _run(
            project_dir=args.project_dir,
            profiles_dir=args.profiles_dir,
            keyword=args.keyword,
            all=args.all,
            select=args.select,
            models=args.models,
            exclude=args.exclude,
            selector=args.selector,
            script=args.scripts,
            before=args.before,
            experimental_ordering=args.experimental_ordering,
            debug=args.debug)


def cli():
    FalCli(sys.argv)


def _run(
    project_dir,
    profiles_dir,
    keyword,
    all,
    select,
    models,
    exclude,
    selector,
    script,
    before,
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
        models = project.get_filtered_models(all, selector_flags, before)
        print_run_info(models, keyword, before)

        if script:
            scripts = []
            for model in models:
                for el in script:
                    scripts.append(FalScript(model, el))
            return run_scripts(scripts, project)

        if experimental_ordering:
            scripts = ScriptGraph(models, keyword, project_dir).sort()
        else:
            scripts = []
            for model in models:
                for path in model.get_script_paths(keyword, real_project_dir, before):
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
