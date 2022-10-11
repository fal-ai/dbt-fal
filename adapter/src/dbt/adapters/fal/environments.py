from __future__ import annotations

import functools
import sys
from functools import partial
from pathlib import Path
from typing import Any, Dict, Iterator

import dbt.exceptions
import dill
import httpx
import yaml
from dbt.config import RuntimeConfig
from dbt.contracts.connection import AdapterResponse
from dbt.parser.manifest import MacroManifest, Manifest
from isolate import prepare_environment
from isolate.backends.connections import ExtendedPythonIPC

from .adapter_support import (
    prepare_for_adapter,
    read_relation_as_df,
    reconstruct_adapter,
    write_df_to_relation,
)
from .connections import FalCredentials


def retrieve_symbol(source_code: str, symbol_name: str) -> Any:
    """Retrieve the function with the given name from the source code."""
    namespace = {}
    exec(source_code, namespace)
    return namespace[symbol_name]


def read_env_definition(project_root: str, environment_name: str) -> Dict[str, Any]:
    """Fetch the environment with the given name from the project's
    fal_project.yml file."""

    fal_project = Path(project_root) / "fal_project.yml"
    if not fal_project.exists():
        raise dbt.exceptions.RuntimeException(
            f"Can't access environment {environment_name} since "
            f"fal_project.yml does not exist under {project_root}"
        )

    with open(fal_project) as stream:
        fal_project = yaml.safe_load(stream)

    if environment_name == "local":
        return {"kind": "local"}

    for environment in fal_project.get("environments", []):
        if "name" not in environment or "kind" not in environment:
            raise dbt.exceptions.RuntimeException(
                f"Invalid environment definition in fal_project.yml: {environment} (name and kind fields are required)"
            )

        if environment["name"] == environment_name:
            environment.pop("name")
            if environment["kind"] == "venv":
                # Alias venv to virtualenv, as isolate calls it.
                environment["kind"] = "virtualenv"

            return environment
    else:
        raise dbt.exceptions.RuntimeException(
            f"Environment '{environment_name}' was used but not defined in fal_project.yml"
        )


def check_isolate_server(func):
    @functools.wraps(func)
    def wrapper(session, *args, **kwargs):
        try:
            return func(session, *args, **kwargs)
        except httpx.ConnectError:
            raise dbt.exceptions.RuntimeException(
                f"Can't seem to access the given fal server server.\n"
                f"Ensure that it is running at {session.base_url}.\n"
                f"See <docs link> for setting up your own server."
            )

    return wrapper


@check_isolate_server
def create_environment(
    session: httpx.Client, kind: str, configuration: Dict[str, Any]
) -> str:
    """Create a new environment with the given definition."""
    response = session.post(
        "/environments",
        json={"kind": kind, "configuration": configuration},
    )
    response.raise_for_status()

    data = response.json()
    assert data["status"] == "success", data["status"]
    return data["token"]


@check_isolate_server
def run_function(session: httpx.Client, environment_token: str, function: Any) -> None:
    """Run the given function in the given environment."""
    response = session.post(
        "/environments/runs",
        params={
            "environment_token": environment_token,
            "serialization_backend": "dill",
        },
        data=dill.dumps(function),
    )
    response.raise_for_status()

    data = response.json()
    assert data["status"] == "success", data["status"]
    return data["token"]


@check_isolate_server
def check_run_status(
    session: httpx.Client, token: str, logs_start: int = 0
) -> Dict[str, Any]:
    """Check the status of a run."""
    response = session.get(
        f"/environments/runs/{token}/status",
        params={
            "logs_start": logs_start,
        },
    )
    response.raise_for_status()

    data = response.json()
    assert data["status"] == "success", data["status"]
    return data


def iter_logs(session: httpx.Client, token: str) -> Iterator[Dict[str, Any]]:
    """Iterate over the logs of a run."""
    logs_start = 0
    while True:
        data = check_run_status(session, token, logs_start)
        for log in data["logs"]:
            yield log

        logs_start += len(data["logs"])
        if data["is_done"]:
            break


def _isolated_runner(
    code: str,
    config: RuntimeConfig,
    manifest: Manifest,
    macro_manifest: MacroManifest,
) -> AdapterResponse:
    """Run the given 'code' with a database adapter constructed from the
    concrete config and manifest objects."""

    adapter = reconstruct_adapter(config, manifest, macro_manifest)
    main = retrieve_symbol(code, "main")
    return main(
        read_df=prepare_for_adapter(adapter, read_relation_as_df),
        write_df=prepare_for_adapter(adapter, write_df_to_relation),
    )


def run_on_host_machine(
    credentials: FalCredentials,
    kind: str,
    configuration: Dict[str, Any],
    code: str,
    model_state: Dict[str, Any],
) -> AdapterResponse:
    """Run the given code on a remote machine (through fal-isolate-server)."""
    with httpx.Client(base_url=credentials.host) as session:
        # User's environment
        environment_token = create_environment(session, kind, configuration)

        # Start running the entrypoint function in the remote environment.
        status_token = run_function(
            session,
            environment_token,
            partial(_isolated_runner, code, **model_state),
        )

        for log in iter_logs(session, status_token):
            if log["source"] == "user":
                print(f"[{log['level']}]", log["message"])
            elif log["source"] == "builder":
                print(f"[environment builder] [{log['level']}]", log["message"])
            elif log["source"] == "bridge":
                print(f"[environment bridge] [{log['level']}]", log["message"])

        # TODO: we should somehow tell whether the run was successful or not.
        return AdapterResponse("OK")


def run_on_local_machine(
    credentials: FalCredentials,
    kind: str,
    configuration: Dict[str, Any],
    code: str,
    model_state: Dict[str, Any],
) -> AdapterResponse:
    """Run the given code on the local machine (in a different process)."""

    isolate_environment = prepare_environment(kind, **configuration)
    environment_connection = isolate_environment.create()
    with ExtendedPythonIPC(
        isolate_environment,
        environment_connection,
        extra_inheritance_paths=[Path(sys.exec_prefix)],
    ) as connection:
        return connection.run(
            partial(_isolated_runner, code, **model_state),
        )
