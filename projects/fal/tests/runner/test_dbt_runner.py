from __future__ import annotations

import os

from tests.graph.utils import assert_contains_only

from fal.cli.args import parse_args
from fal.cli.dbt_runner import get_dbt_command_list


def test_get_dbt_command_list_with_select():
    parsed = parse_args(["flow", "run"])
    models = ["modelA", "modelB"]
    command_list = get_dbt_command_list(parsed, models)
    assert_contains_only(
        command_list,
        [
            "run",
            "--threads",
            "1",
            "--select",
            "modelA",
            "modelB",
            "--project-dir",
            str(os.getcwd()),
        ],
    )


def test_get_dbt_command_list_with_empty_models_list():
    parsed = parse_args(["flow", "run"])
    models = []
    command_list = get_dbt_command_list(parsed, models)
    assert_contains_only(
        command_list,
        [
            "run",
            "--threads",
            "1",
            "--project-dir",
            str(os.getcwd()),
        ],
    )
