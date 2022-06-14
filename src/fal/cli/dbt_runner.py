from pathlib import Path
from typing import Any, Dict, Optional, List
import subprocess
import json
import faldbt.lib as lib
from dbt.logger import GLOBAL_LOGGER as logger
import os
import shutil
from os.path import exists
import tempfile
import argparse


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


def raise_for_dbt_run_errors(output: DbtCliOutput):
    if output.return_code != 0:
        raise RuntimeError("Error running dbt run")


def get_dbt_command_list(args: argparse.Namespace, models_list: List[str]) -> List[str]:
    command_list = ["dbt", "--log-format", "json"]

    if args.debug:
        command_list += ["--debug"]

    command_list += ["run"]

    if args.project_dir:
        command_list += ["--project-dir", args.project_dir]
    if args.profiles_dir:
        command_list += ["--profiles-dir", args.profiles_dir]

    if args.threads:
        command_list += ["--threads", args.threads]

    if args.defer:
        command_list += ["--defer"]

    if args.state:
        command_list += ["--state", args.state]

    if args.target:
        command_list += ["--target", args.target]

    if args.vars is not None and args.vars != "{}":
        command_list += ["--vars", args.vars]

    if len(models_list) > 0:
        if lib.DBT_VCURRENT.compare(lib.DBT_V1) < 0:
            command_list += ["--models"] + models_list
        else:
            command_list += ["--select"] + models_list

    # Assure all command parts are str
    return list(map(str, command_list))


def dbt_run(
    args: argparse.Namespace,
    models_list: List[str],
    target_path: str,
    run_index: int,
    use_temp_dirs: bool = False,
):
    "Run the dbt run command in a subprocess"

    command_list = get_dbt_command_list(args, models_list)

    # Execute the dbt CLI command in a subprocess.
    full_command = " ".join(command_list)

    logger.info(f"Executing command: {full_command}")

    return_code = 0
    logs = []
    output = []

    extra_args = {}
    if use_temp_dirs:
        extra_args['env'] = os.environ.copy()
        extra_args['env']['temp_dir'] = temp_dir = tempfile.mkdtemp()

    process = subprocess.Popen(
        command_list,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        **extra_args
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

    base_dir, target_dir_name = os.path.split(target_path)
    if use_temp_dirs:
        base_dir = temp_dir

    run_results_file = os.path.join(base_dir, target_dir_name, "run_results.json")
    _create_fal_result_file(run_results_file, target_path, run_index)

    # Remove run_result.json files in between dbt runs during the same fal flow run
    if use_temp_dirs:
        shutil.rmtree(temp_dir)
    else:
        os.remove(run_results_file)

    return DbtCliOutput(
        command=full_command,
        return_code=return_code,
        raw_output=raw_output,
        logs=logs,
    )



def _create_fal_result_file(run_results_file: str, target_path: str, run_index: int):
    if exists(run_results_file):
        shutil.copy(
            run_results_file, os.path.join(target_path, f"fal_results_{run_index}.json")
        )
