import functools
from typing import Any, Dict, Iterator

import dbt.exceptions
import dill
import httpx


def check_isolate_server(func):
    @functools.wraps(func)
    def wrapper(api_base, *args, **kwargs):
        try:
            return func(api_base, *args, **kwargs)
        except httpx.ConnectError:
            raise dbt.exceptions.RuntimeException(
                f"Can't seem to access the given fal server server. Ensure that it is running at {api_base}"
            )

    return wrapper


@check_isolate_server
def create_environment(api_base: str, kind: str, configuration: Dict[str, Any]) -> str:
    """Create a new environment with the given definition."""
    response = httpx.post(
        api_base + "/environments/create",
        json={"kind": kind, "configuration": configuration},
    )
    response.raise_for_status()

    data = response.json()
    assert data["status"] == "success", data["status"]
    return data["token"]


@check_isolate_server
def run_function(api_base: str, environment_token: str, function: Any) -> None:
    """Run the given function in the given environment."""
    response = httpx.post(
        api_base + "/environments/runs",
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
def check_run_status(api_base: str, token: str, logs_start: int = 0) -> Dict[str, Any]:
    """Check the status of a run."""
    response = httpx.get(
        api_base + f"/environments/runs/{token}/status",
        params={
            "logs_start": logs_start,
        },
    )
    response.raise_for_status()

    data = response.json()
    assert data["status"] == "success", data["status"]
    return data


def iter_logs(api_base: str, token: str) -> Iterator[Dict[str, Any]]:
    """Iterate over the logs of a run."""
    logs_start = 0
    while True:
        data = check_run_status(api_base, token, logs_start)
        for log in data["logs"]:
            yield log

        logs_start += len(data["logs"])
        if data["is_done"]:
            break
