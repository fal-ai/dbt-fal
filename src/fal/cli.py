import argparse
import os
import sys
import pkg_resources

import dbt.exceptions
import dbt.ui

from dbt.logger import log_manager, GLOBAL_LOGGER as logger
from dbt.config.profile import DEFAULT_PROFILES_DIR

from fal.run_scripts import run_global_scripts, run_scripts
from fal.dag import FalScript, ScriptGraph
from fal.utils import print_run_info
from faldbt.project import FalDbt, FalGeneralException, FalProject


def run_fal(argv):

    # fmt: off
    parser = argparse.ArgumentParser(
        description="Run Python scripts on dbt models",
        usage="""fal COMMAND [<args>]""",
        prog="fal",
    )

    if len(argv) < 2:
        print("No command supplied\n")
        parser.print_help()
        exit(1)

    # Handle version checking
    version = pkg_resources.get_distribution("fal").version
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"fal {version}",
        help="show fal version",
    )

    # Handle commands
    command_parsers = parser.add_subparsers(title="commands", dest="command_parsers")
    run_parser = command_parsers.add_parser(
        "run",
        help="Run Python scripts as final nodes",
    )

    run_parser.add_argument(
        "--project-dir",
        default=os.getcwd(),
        help="Directory to look for dbt_project.yml.",
    )
    run_parser.add_argument(
        "--profiles-dir",
        default=None,
        help="Directory to look for profiles.yml.",
    )
    run_parser.add_argument(
        "--keyword",
        default="fal",
        help="Property in meta to look for fal configurations.",
    )

    run_parser.add_argument(
        "--all",
        action="store_true",
        help="Run scripts for all models. By default, fal runs scripts for models that ran in the last dbt run.",
    )
    # TODO: remove `action="extend"` to match exactly what dbt does
    run_parser.add_argument(
        "-s", "--select",
        nargs="+",
        action="extend", # For backwards compatibility with past fal version
        dest="select",
        help="Specify the nodes to include.",
    )
    run_parser.add_argument(
        "-m", "--models",
        nargs="+",
        action="extend", # For backwards compatibility with past fal version
        dest="select",
        help="Specify the nodes to include.",
    )
    run_parser.add_argument(
        "--exclude",
        nargs="+",
        action="extend", # For backwards compatibility with past fal version
        help="Specify the nodes to exclude.",
    )
    run_parser.add_argument(
        "--selector",
        help="The selector name to use, as defined in selectors.yml",
    )
    run_parser.add_argument(
        "--scripts",
        nargs="+",
        action="extend", # For backwards compatibility with past fal version
        help="Specify scripts to run, overrides schema.yml",
    )

    run_parser.add_argument(
        "--before",
        action="store_true",
        help="Run scripts specified in model `before` tag",
    )
    run_parser.add_argument(
        "--experimental-ordering",
        action="store_true",
        help="Turns on ordering of the fal scripts.",
    )
    run_parser.add_argument(
        "--debug",
        action="store_true",
        help="Display debug logging during execution.",
    )
    # fmt: on

    args = parser.parse_args(argv[1:])

    # TODO: remove `action="extend"` to match exactly what dbt does
    selects_count = (
        argv.count("-s")
        + argv.count("--select")
        + argv.count("-m")
        + argv.count("--model")
    )

    _run(
        project_dir=args.project_dir,
        profiles_dir=args.profiles_dir,
        keyword=args.keyword,
        all=args.all,
        select=args.select,
        exclude=args.exclude,
        selector=args.selector,
        script=args.scripts,
        before=args.before,
        experimental_ordering=args.experimental_ordering,
        debug=args.debug,
        selects_count=selects_count,
    )


def cli():
    run_fal(sys.argv)


def _run(
    project_dir,
    profiles_dir,
    keyword,
    all,
    select,
    exclude,
    selector,
    script,
    before,
    experimental_ordering,
    debug,
    # TODO: remove `action="extend"` to match exactly what dbt does
    selects_count,
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

        # TODO: remove `action="extend"` to match exactly what dbt does
        if selects_count > 1:
            dbt.exceptions.warn_or_error(
                "Passing multiple --select/--model flags to fal is deprecatd.\n"
                + f"Please use model selection like dbt. Use: --select {' '.join(select)}",
                log_fmt=dbt.ui.warning_tag("{}"),
            )

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
