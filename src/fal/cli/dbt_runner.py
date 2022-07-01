import multiprocessing
from multiprocessing import Pool
from typing import Any, Dict, Optional, List
import subprocess
import json
import faldbt.lib as lib
from dbt.logger import GLOBAL_LOGGER as logger
import os
import shutil
import traceback
from os.path import exists
import argparse


class DbtCliOutput:
    def __init__(
        self,
        command: str,
        return_code: int,
        raw_output: Optional[str],
        logs: Optional[List[Dict[str, Any]]],
        run_results: Dict[str, Any],
    ):
        self._command = command
        self._return_code = return_code
        self._raw_output = raw_output
        self._logs = logs
        self._run_results = run_results

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
    def raw_output(self) -> Optional[str]:
        return self._raw_output

    @property
    def logs(self) -> Optional[List[Dict[str, Any]]]:
        return self._logs

    @property
    def run_results(self) -> Dict[str, Any]:
        return self._run_results


def raise_for_dbt_run_errors(output: DbtCliOutput):
    if output.return_code != 0:
        raise RuntimeError("Error running dbt run")


def get_dbt_command_list(
    args: argparse.Namespace, models_list: List[str], include_cli_only_args: bool = True
) -> List[str]:
    if include_cli_only_args:
        command_list = ["dbt", "--log-format", "json"]
    else:
        command_list = []

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
        if lib.IS_DBT_V0:
            command_list += ["--models"] + models_list
        else:
            command_list += ["--select"] + models_list

    # Assure all command parts are str
    return list(map(str, command_list))


def dbt_run(
    args: argparse.Namespace, models_list: List[str], target_path: str, run_index: int
):
    "Run the dbt run command in a subprocess"

    command_list = get_dbt_command_list(args, models_list)

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

    _create_fal_result_file(target_path, run_index)

    run_results = _get_index_run_results(target_path, run_index)

    # Remove run_result.json files in between dbt runs during the same fal flow run
    os.remove(_get_run_result_file(target_path))

    return DbtCliOutput(
        command=full_command,
        return_code=return_code,
        raw_output=raw_output,
        logs=logs,
        run_results=run_results,
    )


def _get_run_result_file(target_path: str) -> str:
    return os.path.join(target_path, "run_results.json")


def _create_fal_result_file(target_path: str, run_index: int):
    fal_run_result = _get_run_result_file(target_path)
    if exists(fal_run_result):
        shutil.copy(
            fal_run_result, os.path.join(target_path, f"fal_results_{run_index}.json")
        )


# This is the Python implementation of the `dbt_run()` function, in which
# we directly use dbt-core as a Python library. We don't run it directly
# but rather use 'multiprocessing' to run it in a real system Process to
# imitate the existing behavior of `dbt_run()` (in terms of performance).

def _dbt_run_through_python(args: List[str], target_path: str, run_index: int):
    from dbt.main import handle_and_check

    run_results = exc = None
    try:
        run_results, success = handle_and_check(args)
    except BaseException as _exc:
        return_code = getattr(exc, "code", 1)
        exc = _exc
    else:
        return_code = 0 if success else 1

    logger.debug(f"dbt exited with return code {return_code}")

    # The 'run_results' object has a 'write()' method which is basically json.dump().
    # We'll dump it directly to the fal results file (instead of first dumping it to
    # run results and then copying it over).
    if run_results is not None:
        run_results.write(os.path.join(target_path, f"fal_results_{run_index}.json"))
    else:
        raise RuntimeError("Error running dbt run") from exc

    return return_code


def dbt_run_through_python(
    args: argparse.Namespace, models_list: List[str], target_path: str, run_index: int
) -> DbtCliOutput:
    """Run DBT from the Python entry point in a subprocess."""
    # dbt-core is currently using the spawn as its mulitprocessing context
    # so we'll mirror it.
    if multiprocessing.get_start_method() != "spawn":
        multiprocessing.set_start_method("spawn", force=True)

    args = get_dbt_command_list(args, models_list, include_cli_only_args=False)
    args_as_text = " ".join(args)
    logger.info(f"Running DBT with these options: {args_as_text}")

    # We are going to use multiprocessing module to spawn a new
    # process that will run the sub-function we have here. We
    # don't really need a process pool, since we are only going
    # to spawn a single process but it provides a nice abstraction
    # over retrieving values from the process.

    with Pool(processes=1) as pool:
        return_code = pool.apply(
            _dbt_run_through_python,
            (args, target_path, run_index),
        )

    run_results = _get_index_run_results(target_path, run_index)
    return DbtCliOutput(
        command="dbt " + args_as_text,
        return_code=return_code,
        raw_output=None,
        logs=None,
        run_results=run_results,
    )


def _get_index_run_results(target_path: str, run_index: int) -> Dict[Any, Any]:
    """Get run results for a given run index."""
    with open(
        os.path.join(target_path, f"fal_results_{run_index}.json")
    ) as raw_results:
        return json.load(raw_results)
