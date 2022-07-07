import ast
import re
from typing import List, cast
from pathlib import Path
from fal.fal_script import python_from_file

from faldbt.parse import load_dbt_project_contract
from fal.cli.model_generator.module_check import (
    generate_dbt_dependencies,
    write_to_model_check,
)

from dbt.logger import GLOBAL_LOGGER as logger

SQL_MODEL_TEMPLATE = """
{{ config(materialized='ephemeral') }}
/*
FAL_GENERATED __checksum__

Script dependencies:

__deps__

*/

SELECT * FROM {{ this }}
"""


def generate_python_dbt_models(project_dir: str):
    project_contract = load_dbt_project_contract(project_dir)
    project_path = Path(project_dir)
    model_paths = map(
        project_path.joinpath, cast(List[str], project_contract.model_paths)
    )

    python_paths: List[Path] = []
    for model_path in model_paths:
        python_paths.extend(_generate_python_dbt_models(model_path))

    return dict([(path.stem, path) for path in python_paths])


GENERATED_DIR = Path("fal")


def _generate_python_dbt_models(model_path: Path):
    python_paths = _find_python_files(model_path)

    for py_path in python_paths:
        # models/staging/model.py -> models/fal/staging/model.sql
        py_relative_path = py_path.relative_to(model_path)
        sql_relative_path = GENERATED_DIR.joinpath(py_relative_path).with_suffix(".sql")
        sql_path = model_path.joinpath(sql_relative_path)

        old_checksum = _check_path_safe_to_write(sql_path, py_path)

        source_code = python_from_file(py_path)
        module = ast.parse(source_code, str(py_path), "exec")

        # Fails if it does not have write_to_model
        write_to_model_check(module)

        dbt_deps: str = generate_dbt_dependencies(module)

        sql_contents = SQL_MODEL_TEMPLATE.replace("__deps__", dbt_deps)
        checksum, _ = _checksum(sql_contents)
        sql_contents = sql_contents.replace("__checksum__", checksum)

        sql_path.parent.mkdir(parents=True, exist_ok=True)
        with open(sql_path, "w") as file:
            file.write(sql_contents)

        if not old_checksum or checksum != old_checksum:
            logger.warn(
                f"File '{sql_relative_path}' was generated from '{py_relative_path}'.\n"
                "Please do not modify it directly. We recommend committing it to your repository."
            )

    return python_paths


# TODO: unit tests
def _check_path_safe_to_write(sql_path: Path, py_path: Path):
    if sql_path.exists():
        with open(sql_path, "r") as file:
            contents = file.read()
            checksum, found = _checksum(contents)
            if not found or checksum != found:
                logger.debug(
                    f"Existing file calculated checksum: {checksum}\nFound checksum: {found}"
                )
                raise RuntimeError(
                    f"File '{sql_path}' not generated by fal would be overwritten by generated model of '{py_path}'. Please rename or remove."
                )
            return checksum


CHECKSUM_REGEX = re.compile(r"FAL_GENERATED ([_\d\w]+)")


def _checksum(contents: str):
    import hashlib

    found = CHECKSUM_REGEX.search(contents)
    to_check = CHECKSUM_REGEX.sub("FAL_GENERATED", contents.strip())
    return (
        hashlib.md5(to_check.encode("utf-8")).hexdigest(),
        found.group(1) if found else None,
    )


def _find_python_files(model_path: Path) -> List[Path]:
    py_files = model_path.rglob("*.py")
    nb_files = model_path.rglob("*.ipynb")
    files = [*py_files, *nb_files]
    return [p for p in files if p.is_file()]
