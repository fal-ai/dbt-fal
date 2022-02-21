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
from faldbt.lib import DBT_VCURRENT, DBT_V1
from faldbt.project import FalDbt, FalGeneralException, FalProject


def _build_run_parser(sub: argparse.ArgumentParser):

    # fmt: off
    sub.add_argument(
        "--project-dir",
        default=os.getcwd(),
        help="Directory to look for dbt_project.yml.",
    )
    sub.add_argument(
        "--profiles-dir",
        default=None,
        help="Directory to look for profiles.yml.",
    )
    sub.add_argument(
        "--keyword",
        default="fal",
        help="Property in meta to look for fal configurations.",
    )

    sub.add_argument(
        "--all",
        action="store_true",
        help="Run scripts for all models. By default, fal runs scripts for models that ran in the last dbt run.",
    )
    # TODO: remove `action="extend"` to match exactly what dbt does
    sub.add_argument(
        "-s", "--select",
        nargs="+",
        action="extend", # For backwards compatibility with past fal version
        dest="select",
        help="Specify the nodes to include.",
    )
    sub.add_argument(
        "-m", "--models",
        nargs="+",
        action="extend", # For backwards compatibility with past fal version
        dest="select",
        help="Specify the nodes to include.",
    )
    sub.add_argument(
        "--exclude",
        nargs="+",
        action="extend", # For backwards compatibility with past fal version
        help="Specify the nodes to exclude.",
    )
    sub.add_argument(
        "--selector",
        help="The selector name to use, as defined in selectors.yml",
    )
    sub.add_argument(
        "--scripts",
        nargs="+",
        action="extend", # For backwards compatibility with past fal version
        help="Specify scripts to run, overrides schema.yml",
    )

    sub.add_argument(
        "--before",
        action="store_true",
        help="Run scripts specified in model `before` tag",
    )
    sub.add_argument(
        "--experimental-ordering",
        action="store_true",
        help="Turns on ordering of the fal scripts.",
    )
    sub.add_argument(
        "--debug",
        action="store_true",
        help="Display debug logging during execution.",
    )
    sub.add_argument(
        "--disable-logging",
        action="store_true",
        help="Disable logging.",
    )
    # fmt: on


def _build_flow_parser(sub: argparse.ArgumentParser):

    flow_command_parsers = sub.add_subparsers(
        title="flow commands",
        dest="flow_command",
        metavar="COMMAND",
        required=True,
    )
    flow_run_parser = flow_command_parsers.add_parser(
        name="run",
        help="Run dbt and fal in order",
    )
    _build_run_parser(flow_run_parser)


def cli(argv=sys.argv):
    parser = argparse.ArgumentParser(
        prog="fal",
        description="Run Python scripts on dbt models",
    )

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
    command_parsers = parser.add_subparsers(
        title="commands",
        dest="command",
        metavar="COMMAND",
        required=True,
    )

    run_parser = command_parsers.add_parser(
        name="run",
        help="Run Python scripts as final nodes",
    )
    _build_run_parser(run_parser)

    flow_parser = command_parsers.add_parser(
        name="flow",
        help="Flow between tools naturally",
    )
    _build_flow_parser(flow_parser)

    args = parser.parse_args(argv[1:])

    # TODO: remove `action="extend"` to match exactly what dbt does
    selects_count = (
        argv.count("-s")
        + argv.count("--select")
        + argv.count("-m")
        + argv.count("--model")
    )

    if args.command == "flow":
        if args.flow_command == "run":
            _dbt_run(args)
            _fal_run(args, selects_count=selects_count)

    elif args.command == "run":
        _fal_run(args, selects_count=selects_count)


def _dbt_run(args):
    # TODO: call dbt cli
    pass


def _fal_run(
    args,
    selects_count,  # TODO: remove `action="extend"` to match exactly what dbt does
):

    if args.disable_logging:
        logger.disable()

    # Re-enable logging for 1.0.0 through old API of logger
    elif DBT_VCURRENT.compare(DBT_V1) >= 0:
        if logger.disabled:
            logger.enable()

    with log_manager.applicationbound():
        if args.debug:
            log_manager.set_debug()

        real_project_dir = os.path.realpath(os.path.normpath(args.project_dir))
        real_profiles_dir = None
        if args.profiles_dir is not None:
            real_profiles_dir = os.path.realpath(os.path.normpath(args.profiles_dir))
        elif os.getenv("DBT_PROFILES_DIR"):
            real_profiles_dir = os.path.realpath(
                os.path.normpath(os.getenv("DBT_PROFILES_DIR"))
            )
        else:
            real_profiles_dir = DEFAULT_PROFILES_DIR

        selector_flags = args.select or args.exclude or args.selector
        if args.all and selector_flags:
            raise FalGeneralException(
                "Cannot pass --all flag alongside selection flags (--select/--models, --exclude, --selector)"
            )

        faldbt = FalDbt(
            real_project_dir,
            real_profiles_dir,
            args.select,
            args.exclude,
            args.selector,
            args.keyword,
        )
        project = FalProject(faldbt)
        models = project.get_filtered_models(args.all, selector_flags, args.before)

        # TODO: remove `action="extend"` to match exactly what dbt does
        if selects_count > 1:
            dbt.exceptions.warn_or_error(
                "Passing multiple --select/--model flags to fal is deprecatd.\n"
                + f"Please use model selection like dbt. Use: --select {' '.join(args.select)}",
                log_fmt=dbt.ui.warning_tag("{}"),
            )

        print_run_info(models, args.keyword, args.before)

        if args.scripts:
            scripts = []
            for model in models:
                for el in args.scripts:
                    scripts.append(FalScript(model, el))
            return run_scripts(scripts, project)

        if args.experimental_ordering:
            scripts = ScriptGraph(models, args.keyword, args.project_dir).sort()
        else:
            scripts = []
            for model in models:
                for path in model.get_script_paths(
                    args.keyword, real_project_dir, args.before
                ):
                    scripts.append(FalScript(model, path))

        # run model specific scripts first
        run_scripts(scripts, project)

        # then run global scripts
        global_key = "before" if args.before else "after"
        global_scripts = list(
            map(
                lambda path: FalScript(None, path, []),
                faldbt._global_script_paths[global_key],
            )
        )

        run_global_scripts(global_scripts, project)
