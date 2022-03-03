from typing import Any, Dict, Optional, List
import subprocess
import json

from dbt.logger import log_manager, GLOBAL_LOGGER as logger
from . import DbtCliRuntimeError
from . import DbtCliOutput


def dbt_run(args):
    "Run the dbt run command in a subprocess"

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
