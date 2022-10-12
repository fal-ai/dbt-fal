import multiprocessing
from multiprocessing.connection import Connection
from typing import Any, Dict, Optional, List
import warnings
import json
from faldbt.logger import LOGGER
import os
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


def get_dbt_command_list(args: argparse.Namespace, models_list: List[str]) -> List[str]:
    command_list = []

    if args.debug:
        command_list += ["--debug"]

    command_list += ["run"]

    # NOTE: Safety measure because we do threading on fal
    command_list += ["--threads", str(1)]

    if args.project_dir:
        command_list += ["--project-dir", args.project_dir]
    if args.profiles_dir:
        command_list += ["--profiles-dir", args.profiles_dir]

    if args.defer:
        command_list += ["--defer"]

    if args.state:
        command_list += ["--state", args.state]

    if args.full_refresh:
        command_list += ["--full-refresh"]

    if args.target:
        command_list += ["--target", args.target]

    if args.vars is not None and args.vars != "{}":
        command_list += ["--vars", args.vars]

    if len(models_list) > 0:
        command_list += ["--select"] + models_list

    # Assure all command parts are str
    return list(map(str, command_list))


# This is the Python implementation of the `dbt_run()` function, in which
# we directly use dbt-core as a Python library. We don't run it directly
# but rather use 'multiprocessing' to run it in a real system Process to
# imitate the existing behavior of `dbt_run()` (in terms of performance).


def _dbt_run_through_python(
    args: List[str], target_path: str, run_index: int, connection: Connection
):
    # logbook is currently using deprecated APIs internally, which is causing
    # a crash. We'll mirror the solution from DBT, until it is fixed on
    # upstream.
    #
    # PR from dbt-core: https://github.com/dbt-labs/dbt-core/pull/4866

    warnings.filterwarnings("ignore", category=DeprecationWarning, module="logbook")

    from dbt.main import handle_and_check

    run_results = exc = None
    try:
        run_results, success = handle_and_check(args)
    except BaseException as _exc:
        return_code = getattr(exc, "code", 1)
        exc = _exc
    else:
        return_code = 0 if success else 1

    LOGGER.debug(f"dbt exited with return code {return_code}")

    # The 'run_results' object has a 'write()' method which is basically json.dump().
    # We'll dump it directly to the fal results file (instead of first dumping it to
    # run results and then copying it over).
    if run_results is not None:
        run_results.write(os.path.join(target_path, f"fal_results_{run_index}.json"))
    else:
        connection.send(exc)
        return

    connection.send(return_code)


def dbt_run_through_python(
    args: argparse.Namespace, models_list: List[str], target_path: str, run_index: int
) -> DbtCliOutput:
    """Run DBT from the Python entry point in a subprocess."""
    # dbt-core is currently using the spawn as its mulitprocessing context
    # so we'll mirror it.
    if multiprocessing.get_start_method() != "spawn":
        multiprocessing.set_start_method("spawn", force=True)

    args_list = get_dbt_command_list(args, models_list)

    cmd_str = " ".join(["dbt", *args_list])
    LOGGER.info("Running command: {}", cmd_str)

    # We will be using a multiprocessing.Pipe to communicate
    # from subprocess to main process about the return code
    # as well as the exceptions that might arise.
    p_connection, c_connection = multiprocessing.Pipe()
    process = multiprocessing.Process(
        target=_dbt_run_through_python,
        args=(args_list, target_path, run_index, c_connection),
    )

    process.start()
    result = p_connection.recv()
    if not isinstance(result, int):
        raise RuntimeError("Error running dbt run") from result
    process.join()

    run_results = _get_index_run_results(target_path, run_index)
    return DbtCliOutput(
        command=cmd_str,
        return_code=result,
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
