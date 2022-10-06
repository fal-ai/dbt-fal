from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
import yaml

import dbt.exceptions


def retrieve_symbol(source_code: str, symbol_name: str) -> Any:
    """Retrieve the function with the given name from the source code."""
    namespace = {}
    exec(source_code, namespace)
    return namespace[symbol_name]


def fetch_environment(project_root: str, environment_name: str) -> Dict[str, Any]:
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

    for environment in fal_project.get("environments", []):
        if "name" not in environment or "kind" not in environment:
            raise dbt.exceptions.RuntimeException(
                f"Invalid environment definition in fal_project.yml: {environment} (name and kind fields are required)"
            )

        if environment["name"] == environment_name:
            environment.pop("name")
            return environment
    else:
        raise dbt.exceptions.RuntimeException(
            f"Environment '{environment_name}' was used but not defined in fal_project.yml"
        )
