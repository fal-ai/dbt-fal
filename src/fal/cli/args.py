from uuid import uuid4
from typing import List, Any
import os
import argparse
import pkg_resources


LEVEL_FLAGS = {}


class FalArgsError(Exception):
    pass


class _LevelFlag:
    levels: List[str]
    default: Any

    def __init__(self, default):
        self.levels = []
        self.default = default


# This is to handle offering the same flag at several parser levels
# e.g. fal --profiles-dir ~/somewhere run --profiles-dir ~/other_place
# we should get profiles_dir=~/other_place (right-most wins)
def _flag_level(name: str, default=None):
    level = uuid4()
    # Store initial _LevelFlag value in case there is none yet
    LEVEL_FLAGS[name] = LEVEL_FLAGS.get(name, _LevelFlag(default))
    level_flag = LEVEL_FLAGS[name]
    # Add current level, these are meant to read in order (right-most wins)
    level_flag.levels.append(level)

    if default != level_flag.default:
        raise FalArgsError(
            f"Different defaults '{default}' and '{level_flag.default}' for flag '{name}'"
        )

    return f"{name}_{level}"


# Use right after creating the parser, before adding subparsers to it
def _build_fal_common_options(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--disable-logging",
        action="store_true",
        help="Disable logging.",
        dest=_flag_level("disable_logging"),
    )


# Use right after creating the parser, before adding subparsers to it
def _build_dbt_common_options(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--project-dir",
        metavar="PROJECT_DIR",
        help="Directory to look for dbt_project.yml.",
        dest=_flag_level("project_dir", os.getcwd()),
    )

    parser.add_argument(
        "--profiles-dir",
        metavar="PROFILES_DIR",
        help="Directory to look for profiles.yml.",
        dest=_flag_level("profiles_dir"),
    )

    parser.add_argument(
        "--defer",
        action="store_true",
        help="If set, defer to the state variable for resolving unselected nodes.",
        dest=_flag_level("defer"),
    )


def _add_threads_option(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--threads",
        type=int,
        help="Specify number of threads to use while executing Python scripts and dbt models. Overrides settings in profiles.yml.",
    )


def _add_target_option(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--target",
        type=str,
        default=None,
        help="Specify a custom target from profiles.yml.",
    )


def _add_full_refresh_option(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        default=False,
        help="If specified, fal will pass dbt calls the --full-refresh flag, which will drop incremental models and fully-recalculate the incremental table from the model definition.",
    )


def _add_state_option(parser: argparse.ArgumentParser):
    parser.add_argument("--state", type=str, help="Specify dbt state artifact path")


def _add_vars_option(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--vars",
        type=str,
        default="{}",
        help="""
        Supply variables to the project. This argument overrides variables
        defined in your dbt_project.yml file. This argument should be a YAML
        string, eg. '{my_variable: my_value}'
        """,
    )


def _add_experimental_flow_option(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--experimental-flow",
        action="store_true",
        help="DEPRECATED: no-op",
    )


def _add_experimental_python_models_option(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--experimental-models",
        dest="experimental_python_models",
        action="store_true",
        help="DEPRECATED: no-op",
    )


def _add_experimental_threading(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--experimental-threads",
        type=int,
        help="DEPRECATED: no-op",
        metavar="INT",
    )


def _build_dbt_selectors(sub: argparse.ArgumentParser):

    # fmt: off
    sub.add_argument(
        "-s", "--select",
        nargs="+",
        dest="select",
        help="Specify the nodes to include.",
    )
    sub.add_argument(
        "-m", "--models",
        nargs="+",
        dest="select",
        help="Specify the nodes to include.",
    )
    sub.add_argument(
        "--selector",
        help="The selector name to use, as defined in selectors.yml",
    )
    sub.add_argument(
        "--exclude",
        nargs="+",
        help="Specify the nodes to exclude.",
    )
    # fmt: on


def _build_run_parser(sub: argparse.ArgumentParser):

    # fmt: off
    _build_dbt_selectors(sub)
    _build_dbt_common_options(sub)
    _build_fal_common_options(sub)
    _add_threads_option(sub)
    _add_target_option(sub)

    sub.add_argument(
        "--all",
        action="store_true",
        help="Run scripts for all models. By default, fal runs scripts for models that ran in the last dbt run.",
    )
    sub.add_argument(
        "--scripts",
        nargs="+",
        help="Specify scripts to run, overrides schema.yml",
    )

    sub.add_argument(
        "--before",
        action="store_true",
        help="Run scripts specified in model `before` tag",
    )

    sub.add_argument(
        "--globals",
        action="store_true",
        default=False,
        help="Run global scripts along with selected scripts",
    )
    # fmt: on


def _build_flow_parser(sub: argparse.ArgumentParser):

    flow_command_parsers = sub.add_subparsers(
        title="flow commands",
        dest="flow_command",
        metavar="COMMAND",
        required=True,
    )
    _build_dbt_common_options(sub)
    _build_fal_common_options(sub)

    flow_run_parser = flow_command_parsers.add_parser(
        name="run",
        help="Execute fal and dbt run in correct order",
    )
    _build_dbt_selectors(flow_run_parser)
    _build_dbt_common_options(flow_run_parser)
    _build_fal_common_options(flow_run_parser)
    _add_threads_option(flow_run_parser)
    _add_state_option(flow_run_parser)
    _add_experimental_flow_option(flow_run_parser)
    _add_experimental_python_models_option(flow_run_parser)
    _add_experimental_threading(flow_run_parser)
    _add_vars_option(flow_run_parser)
    _add_target_option(flow_run_parser)
    _add_full_refresh_option(flow_run_parser)


def _build_cli_parser():
    parser = argparse.ArgumentParser(
        prog="fal",
        description="Run Python scripts on dbt models",
    )

    version = pkg_resources.get_distribution("fal").version
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"fal {version}",
        help="show fal version",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Display debug logging during execution.",
    )

    _build_dbt_common_options(parser)
    _build_fal_common_options(parser)

    # Handle commands
    command_parsers = parser.add_subparsers(
        title="commands",
        dest="command",
        metavar="COMMAND",
        required=True,
    )

    run_parser = command_parsers.add_parser(
        name="run",
        help="Run Python scripts as post-hook nodes",
    )
    _build_run_parser(run_parser)

    flow_parser = command_parsers.add_parser(
        name="flow",
        help="Execute fal and dbt commands in correct order",
    )
    _build_flow_parser(flow_parser)

    return parser


cli_parser = _build_cli_parser()


def parse_args(argv: List[str]) -> argparse.Namespace:
    args = cli_parser.parse_args(argv)
    args_dict = vars(args)

    # Reduce level flags into a single one with the value to use
    for name, level_flag in LEVEL_FLAGS.items():
        args_dict[name] = level_flag.default
        for level in level_flag.levels:
            # Read and delete the level flag to keep only the main one
            current = args_dict.pop(f"{name}_{level}", None)
            if current is not None:
                args_dict[name] = current

    # Build new argparse.Namespace with the correct flags
    return argparse.Namespace(**args_dict)
