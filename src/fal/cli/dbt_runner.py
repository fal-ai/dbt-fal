from typing import Any, Dict, Optional, List
import subprocess
import json
import faldbt.lib as lib
from dbt.logger import GLOBAL_LOGGER as logger


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


def dbt_run(args, models_list: List[str]):
    "Run the dbt run command in a subprocess"

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

    if args.select:
        if lib.DBT_VCURRENT.compare(lib.DBT_V1) < 0:
            command_list += ["--model"] + models_list
        else:
            command_list += ["--select"] + models_list
    if args.selector:
        command_list += ["--selector", args.selector]

    command_list = list(map(str, command_list)) # make sure all are strings before joining for printing

    # Execute the dbt CLI command in a subprocess.
    full_command = " ".join(command_list) # make sure all are strings before joining for printing
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
