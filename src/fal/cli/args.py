from dataclasses import dataclass
from email.policy import default
from uuid import uuid4
from typing import List
import os
import argparse
import pkg_resources


LEVEL_FLAGS = {}


@dataclass(init=False)
class _LevelFlag:
    levels: List[str]
    default: any

    def __init__(self, default):
        self.levels = []
        self.default = default


def _flag_level(name: str, default=None):
    level = uuid4()
    LEVEL_FLAGS[name] = LEVEL_FLAGS.get(name, _LevelFlag(default))
    level_flag = LEVEL_FLAGS[name]
    level_flag.levels.append(level)

    if default != level_flag.default:
        print(
            f"ERROR: different defaults {default} and {level_flag.default} for flag {name}"
        )

    return f"{name}_{level}"


def _build_fal_common_options(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--disable-logging",
        action="store_true",
        help="Disable logging.",
        dest=_flag_level("disable_logging"),
    )

    parser.add_argument(
        "--keyword",
        help="Property in dbt relations meta to look for fal configurations.",
        dest=_flag_level("keyword", "fal"),
    )


def _build_dbt_common_options(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--project-dir",
        help="Directory to look for dbt_project.yml.",
        dest=_flag_level("project_dir", os.getcwd()),
    )

    parser.add_argument(
        "--profiles-dir",
        help="Directory to look for profiles.yml.",
        dest=_flag_level("profiles_dir"),
    )


def _build_dbt_selectors(sub: argparse.ArgumentParser, extend: bool):

    # fmt: off
    # TODO: remove `action="extend"` to match exactly what dbt does
    sub.add_argument(
        "-s", "--select",
        nargs="+",
        action="extend" if extend else None, # For backwards compatibility with past fal version
        dest="select",
        help="Specify the nodes to include.",
    )
    sub.add_argument(
        "-m", "--models",
        nargs="+",
        action="extend" if extend else None, # For backwards compatibility with past fal version
        dest="select",
        help="Specify the nodes to include.",
    )
    sub.add_argument(
        "--exclude",
        nargs="+",
        action="extend" if extend else None, # For backwards compatibility with past fal version
        help="Specify the nodes to exclude.",
    )
    sub.add_argument(
        "--selector",
        help="The selector name to use, as defined in selectors.yml",
    )
    # fmt: on


def _build_run_parser(sub: argparse.ArgumentParser):

    # fmt: off
    # TODO: remove `action="extend"` to match exactly what dbt does
    _build_dbt_selectors(sub, extend=True)
    _build_dbt_common_options(sub)
    _build_fal_common_options(sub)

    sub.add_argument(
        "--all",
        action="store_true",
        help="Run scripts for all models. By default, fal runs scripts for models that ran in the last dbt run.",
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
        help="Run dbt and fal in order",
    )
    _build_dbt_selectors(flow_run_parser, extend=False)
    _build_dbt_common_options(flow_run_parser)
    _build_fal_common_options(flow_run_parser)


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
        help="Run Python scripts as final nodes",
    )
    _build_run_parser(run_parser)

    flow_parser = command_parsers.add_parser(
        name="flow",
        help="Flow between tools naturally",
    )
    _build_flow_parser(flow_parser)

    return parser


cli_parser = _build_cli_parser()


def parse_args(argv: List[str]):
    parsed = cli_parser.parse_args(argv)
    parsed_dict = vars(parsed)

    # Handle repeating args
    for name, level_flag in LEVEL_FLAGS.items():
        parsed_dict[name] = level_flag.default
        for level in level_flag.levels:
            current = parsed_dict.pop(f"{name}_{level}", None)
            if current:
                parsed_dict[name] = current
    return argparse.Namespace(**parsed_dict)
