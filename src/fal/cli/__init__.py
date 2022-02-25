import argparse
from typing import Any, Dict, Optional, List
import os
import sys
import subprocess
import json

import dbt.exceptions
import dbt.ui
from dbt.logger import log_manager, GLOBAL_LOGGER as logger
from dbt.config.profile import DEFAULT_PROFILES_DIR

from fal.run_scripts import run_global_scripts, run_scripts
from fal.fal_script import FalScript
from fal.utils import print_run_info
from faldbt.lib import DBT_VCURRENT, DBT_V1
from faldbt.project import FalDbt, FalGeneralException, FalProject

from .args import build_cli_parser


class DbtCliRuntimeError(Exception):
    pass


class DbtCliOutput:
    def __init__(
        self,
        command: str,
        return_code: int,
        raw_output: str,
        logs: List[Dict[str, Any]],
    ):
        self._command = command
        self._return_code = return_code
        self._raw_output = raw_output
        self._logs = logs

    @property
    def docs_url(self) -> Optional[str]:
        return None

    @property
    def command(self) -> str:
        return self._command

    @property
    def return_code(self) -> int:
        return self._return_code

    @property
    def raw_output(self) -> str:
        return self._raw_output

    @property
    def logs(self) -> List[Dict[str, Any]]:
        return self._logs


def cli(argv=sys.argv):
    parser = build_cli_parser()

    parsed = parser.parse_args(argv[1:])

    # TODO: remove `action="extend"` to match exactly what dbt does
    selects_count = (
        argv.count("-s")
        + argv.count("--select")
        + argv.count("-m")
        + argv.count("--model")
    )
    exclude_count = argv.count("--exclude")
    script_count = argv.count("--script")

    if parsed.disable_logging:
        logger.disable()
    # Re-enable logging for 1.0.0 through old API of logger
    elif DBT_VCURRENT.compare(DBT_V1) >= 0:
        if logger.disabled:
            logger.enable()

    with log_manager.applicationbound():
        if parsed.debug:
            log_manager.set_debug()

        if parsed.command == "flow":
            if parsed.flow_command == "run":
                _dbt_run(parsed)
                _fal_run(parsed)

        elif parsed.command == "run":
            _fal_run(
                parsed,
                selects_count=selects_count,
                exclude_count=exclude_count,
                script_count=script_count,
            )


def _dbt_run(args):
    command_list = ["dbt", "--log-format", "json"]

    if args.debug:
        command_list += ["--debug"]

    command_list += ["run"]

    if args.project_dir:
        command_list += ["--project-dir", args.project_dir]
    if args.profiles_dir:
        command_list += ["--profiles-dir", args.profiles_dir]

    if args.select:
        command_list += ["--select"] + args.select
    if args.exclude:
        command_list += ["--exclude"] + args.exclude
    if args.selector:
        command_list += ["--selector"] + [args.selector]

    # Execute the dbt CLI command in a subprocess.
    full_command = " ".join(command_list)
    logger.info(f"Executing command: {full_command}")

    return_code = 0
    logs = []
    output = []

    process = subprocess.Popen(
        command_list, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )

    for raw_line in process.stdout or []:
        line = raw_line.decode("utf-8")
        output.append(line)
        try:
            json_line = json.loads(line)
        except json.JSONDecodeError:
            logger.error(line.rstrip())
            pass
        else:
            logs.append(json_line)
            logger.info(json_line.get("message", json_line.get("msg", line.rstrip())))

    process.wait()
    return_code = process.returncode

    logger.debug(f"dbt exited with return code {return_code}")

    raw_output = "\n".join(output)

    if return_code == 2:
        raise DbtCliRuntimeError(raw_output)

    if return_code == 1:
        raise DbtCliRuntimeError(raw_output)

    return DbtCliOutput(
        command=full_command,
        return_code=return_code,
        raw_output=raw_output,
        logs=logs,
    )


def _fal_run(
    args: argparse.Namespace,
    selects_count=0,  # TODO: remove `action="extend"` to match exactly what dbt does
    exclude_count=0,
    script_count=0,
):

    args_dict = vars(args)
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
    if args_dict.get("all") and selector_flags:
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
    models = project.get_filtered_models(
        args_dict.get("all"), selector_flags, args_dict.get("before")
    )

    # TODO: remove `action="extend"` to match exactly what dbt does
    if selects_count > 1:
        dbt.exceptions.warn_or_error(
            "Passing multiple --select/--model flags to fal is deprecatd.\n"
            + f"Please use model selection like dbt. Use: --select {' '.join(args.select)}",
            log_fmt=dbt.ui.warning_tag("{}"),
        )
    if exclude_count > 1:
        dbt.exceptions.warn_or_error(
            "Passing multiple --exclude flags to fal is deprecatd.\n"
            + f"Please use model exclusion like dbt. Use: --exclude {' '.join(args.exclude)}",
            log_fmt=dbt.ui.warning_tag("{}"),
        )
    if script_count > 1:
        dbt.exceptions.warn_or_error(
            "Passing multiple --script flags to fal is deprecatd.\n"
            + f"Please use: --script {' '.join(args.scripts)}",
            log_fmt=dbt.ui.warning_tag("{}"),
        )

    scripts = []
    # if --script selector is there only run selected scripts

    if args_dict.get("scripts"):
        scripts = []
        for model in models:
            model_scripts = model.get_scripts(
                args.keyword, args_dict.get("before"))
            for el in args.scripts:
                if el in model_scripts:
                    scripts.append(FalScript(model, el))

        print_run_info(scripts, args.keyword, args_dict.get("before"))
        run_scripts(scripts, project)

    else:
        for model in models:
            for path in model.get_script_paths(
                args.keyword, real_project_dir, args_dict.get("before")
            ):
                scripts.append(FalScript(model, path))

        print_run_info(scripts, args.keyword, args_dict.get("before"))

        # run model specific scripts first
        run_scripts(scripts, project)

        # then run global scripts
        global_key = "before" if args_dict.get("before") else "after"
        global_scripts = list(
            map(
                lambda path: FalScript(None, path),
                faldbt._global_script_paths[global_key],
            )
        )

        run_global_scripts(global_scripts, project)
