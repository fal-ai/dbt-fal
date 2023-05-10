from functools import reduce, partial
import os
import shlex
from typing import List, Optional
from behave import *
import glob
from fal.cli import cli
import tempfile
import json
import unittest
from os.path import exists
from pathlib import Path
from datetime import datetime, timezone
import re


# The main distinction we can use on an artifact file to determine
# whether it was created by a Python script or a Python model is the number
# of suffixes it has. Models use <model_name>.txt and scripts use
# <model_name>.<script_name>.txt

FAL_MODEL = 1
FAL_SCRIPT = 2


def temp_path(context, *paths: str):
    return str(Path(context.temp_dir.name).joinpath(*paths))

def target_path(context, *paths: str):
    return temp_path(context, "target", *paths)

def artifact_glob(context):
    return Path(temp_path(context)).glob("*.txt")

def _command_replace(command: str, context):
    return (
        command.replace("$baseDir", context.base_dir)
        .replace("$profilesDir", str(_set_profiles_dir(context)))
        .replace("$tempDir", temp_path(context))
    )

@when("the following shell command is invoked")
def run_command_step(context):
    command = _command_replace(context.text, context)
    os.system(command)


@given("the project {project}")
def set_project_folder(context, project: str):
    projects_dir = _find_projects_directory()
    project_path = projects_dir / project
    if not project_path.exists() or not project_path.is_dir():
        extra = ""
        try:
            # Try to find the correct option
            match = re.match("^(\\d+)_", project)

            if match:
                project_number = match.group(1)
                found = [r.name for r in projects_dir.glob(project_number + "_*")]
                if found:
                    extra = "Is it " + " or ".join(found) + " ?"
        finally:
            raise ValueError(f"Project {project} not found. {extra}")

    context.base_dir = str(project_path)
    context.temp_dir = tempfile.TemporaryDirectory()
    os.environ["project_dir"] = context.base_dir
    os.environ["temp_dir"] = context.temp_dir.name

    # TODO: re-enable when the issue https://github.com/dbt-labs/dbt-core/issues/7465 is fixed
    # os.environ["DBT_TARGET_PATH"] = target_path(context)


@when("the data is seeded")
def seed_data(context):
    base_path = Path(context.base_dir)
    profiles_dir = _set_profiles_dir(context)
    os.system(
        f"dbt seed --profiles-dir {profiles_dir} --project-dir {base_path} --full-refresh"
    )


@when("the data is seeded to {target} target in profile directory {profiles_dir}")
def seed_data_custom_target(context, target, profiles_dir):
    base_path = Path(context.base_dir)
    os.system(
        f"dbt seed --profiles-dir {profiles_dir} --project-dir {base_path} --full-refresh --target {target}"
    )


@when("state is stored in {folder_name}")
def persist_state(context, folder_name):
    os.system(f"mv {target_path(context)} {temp_path(context, folder_name)}")


@when("the file {file} is created with the content")
def add_model(context, file):
    file = file.replace("$baseDir", context.base_dir)
    context.added_during_tests = (
        context.added_during_tests.append(file)
        if hasattr(context, "added_during_tests")
        else [file]
    )
    parent = os.path.dirname(file)
    if not exists(parent):
        os.mkdir(parent)
    with open(file, "w") as f:
        f.write(context.text)
        f.close()


def _invoke_command(context, command: str):
    _clear_all_artifacts(context)
    args = _command_replace(command, context)
    args_list = shlex.split(args)

    cli(args_list)

@when("the following command is invoked")
def invoke_command(context):
    context.exc = None
    try:
        _invoke_command(context, context.text)
    except BaseException:
        import sys

        context.exc = sys.exc_info()


@then("it throws an exception {etype} with message '{msg}'")
def invoke_command_error(context, etype: str, msg: str):
    # TODO: Somehow capture logging and check the contents for exceptions

    # from behave.log_capture import LoggingCapture
    # from io import StringIO
    # log_cap: LoggingCapture = context.log_capture
    # out_cap: StringIO = context.stdout_capture
    # err_cap: StringIO = context.stderr_capture

    if context.exc:
        _etype, exc, _tb = context.exc
        if isinstance(exc, SystemExit):
            if not exc.code:
                # zero exit code
                raise AssertionError("Should have thrown an exception")
        else:
            assert isinstance(
                exc, eval(etype)
            ), f"Invalid exception - expected {etype}, got {type(exc)}"
            assert msg in str(exc), "Invalid message - expected " + msg
    else:
        raise AssertionError("Should have thrown an exception")

    # Clear the exception
    context.exc = None


@then("the following command will fail")
def invoke_failing_fal_flow(context):
    try:
        _invoke_command(context, context.text)
        assert False, "Command should have failed."
    except Exception as e:
        print(e)


@then("the following scripts are ran")
def check_script_files_exist(context):
    python_scripts = _get_fal_scripts(context)
    expected_scripts = list(map(_script_filename, context.table.headings))
    unittest.TestCase().assertCountEqual(python_scripts, expected_scripts)


@then("the following scripts are not ran")
def check_script_files_dont_exist(context):
    python_scripts = set(_get_fal_scripts(context))
    expected_scripts = set(map(_script_filename, context.table.headings))

    unexpected_runs = expected_scripts & python_scripts
    if unexpected_runs:
        to_report = ", ".join(unexpected_runs)
        assert False, f"Script files {to_report} should NOT BE present"


def _clear_all_artifacts(context):
    """Clear all artifacts that are left behind by Python scripts and models."""
    for artifact in artifact_glob(context):
        artifact.unlink()


@then("the script {script} output file has the lines")
def check_file_has_lines(context, script):
    filename = _script_filename(script)
    with open(temp_path(context, filename)) as handle:
        handle_lines = [line.strip().lower() for line in handle]
        expected_lines = [line.lower() for line in context.table.headings]
        for line in expected_lines:
            assert line in handle_lines, f"{line} not in {handle_lines}"


@then("no models are calculated")
def no_models_are_run(context):
    fal_results = _get_fal_results_file_name(context)
    fal_results_paths = [temp_path(context, res) for res in fal_results]
    for fal_result_path in fal_results_paths:
        if exists(fal_result_path):
            data = json.load(open(fal_result_path))
            assert (
                len(data["results"]) == 0
            ), f"results length is {len(data['results'])}"
        else:
            assert True


@then("no scripts are run")
def no_scripts_are_run(context):
    results = list(artifact_glob(context))

    assert len(results) == 0


@then("the following models are calculated")
def check_model_results(context):
    models = _get_all_models(context)
    expected_models = list(map(_script_filename, context.table.headings))
    unittest.TestCase().assertCountEqual(models, expected_models)


@then("the following models are calculated in order")
def check_model_results(context):
    models = _get_all_models(context)
    expected_models = list(map(_script_filename, context.table.headings))
    unittest.TestCase().assertCountEqual(models, expected_models)
    _verify_node_order(context)

def _find_projects_directory():
    path = Path(__file__)
    while path is not None and not (path / "projects").exists():
        path = path.parent
    return (path / "projects")

def _verify_node_order(context):
    import networkx as nx
    from fal import FalDbt
    from fal.node_graph import NodeGraph

    fal_dbt = FalDbt(
        profiles_dir=_set_profiles_dir(context), project_dir=context.base_dir
    )
    node_graph = NodeGraph.from_fal_dbt(fal_dbt)

    all_nodes = _get_dated_dbt_models(context) + _get_dated_fal_artifacts(
        context, FAL_SCRIPT
    )
    # We need to normalize the suffix for Python models.
    all_nodes += [
        (_as_name(name), date)
        for name, date in _get_dated_fal_artifacts(context, FAL_MODEL)
    ]
    ordered_nodes = _unpack_dated_result(all_nodes)

    graph = node_graph.graph
    ancestors, post_hooks, pre_hooks, descendants = {}, {}, {}, {}
    for node, data in graph.nodes(data=True):
        name = _as_name(node)
        for container, filter_func in [
            (ancestors, nx.ancestors),
            (descendants, nx.descendants),
        ]:
            container[name] = [
                _as_name(ancestor)
                for ancestor in filter_func(graph, node)
                if _as_name(ancestor) in ordered_nodes
            ]

        for container, hook_type in [
            (pre_hooks, "pre_hook"),
            (post_hooks, "post_hook"),
        ]:
            container[name] = [
                _script_filename(hook.path, name)
                for hook in data.get(hook_type, [])
                if _script_filename(hook.path, name) in ordered_nodes
            ]

    assert_precedes = partial(_assert_precedes, ordered_nodes)
    assert_succeeds = partial(_assert_succeeds, ordered_nodes)
    for node in ordered_nodes:
        # Skip all the nodes that are not part of the graph.
        if node not in ancestors:
            continue

        # Ancestors (and their hooks) must precede the node
        for ancestor in ancestors[node]:
            assert_precedes(node, *pre_hooks[ancestor])
            assert_precedes(node, ancestor)
            assert_precedes(node, *post_hooks[ancestor])

        # pre-hooks of the node will precede the node
        assert_precedes(node, *pre_hooks[node])

        # post-hooks of the node will succeed the node
        assert_succeeds(node, *post_hooks[node])

        # Descendants (and their hooks) must succeed the node
        for successor in descendants[node]:
            assert_succeeds(node, *pre_hooks[successor])
            assert_succeeds(node, successor)
            assert_succeeds(node, *post_hooks[successor])


def _assert_succeeds(nodes, node, *successors):
    for successor in successors:
        assert nodes.index(successor) > nodes.index(
            node
        ), f"{successor} must come after {node}"


def _assert_precedes(nodes, node, *predecessors):
    for predecessor in predecessors:
        assert nodes.index(predecessor) < nodes.index(
            node
        ), f"{predecessor} must come before {node}"


def _as_name(node):
    # format for scripts: script.<model>.<direction>.<script_name>
    if node.startswith("script."):
        _, model_name, _, script_name = node.split(".", 3)
        return model_name + "." + script_name
    elif node.endswith(".txt"):
        return node.split(".")[-2]
    else:
        return node.split(".")[-1]


def _script_filename(script: str, model_name: Optional[str] = None):
    script_name = script.replace(".ipynb", ".txt").replace(".py", ".txt")
    if model_name:
        script_name = model_name + "." + script_name
    return script_name


def _get_all_models(context) -> List[str]:
    """Retrieve all models (both DBT and Python)."""
    all_models = _get_dated_dbt_models(context) + _get_dated_fal_artifacts(
        context, FAL_MODEL
    )
    return _unpack_dated_result(all_models)


def _get_fal_scripts(context) -> List[str]:
    return _unpack_dated_result(_get_dated_fal_artifacts(context, FAL_SCRIPT))


def _unpack_dated_result(dated_result) -> List[str]:
    if not dated_result:
        return []

    result, _ = zip(*sorted(dated_result, key=lambda node: node[1]))
    return list(result)


def _get_dated_dbt_models(context):
    return [
        (
            result["unique_id"].split(".")[-1],
            datetime.fromisoformat(
                result["timing"][-1]["completed_at"].replace("Z", "+00:00")
            ),
        )
        for result in _load_dbt_result_file(context)
    ]


def _get_dated_fal_artifacts(context, *kinds):
    assert kinds, "Specify at least one artifact kind."

    return [
        # DBT run result files use UTC as the timezone for the timestamps, so
        # we need to be careful on using the same method for the local files as well.
        (
            artifact.name,
            datetime.fromtimestamp(artifact.stat().st_mtime, tz=timezone.utc),
        )
        for artifact in artifact_glob(context)
        if len(artifact.suffixes) in kinds
    ]


def _load_dbt_result_file(context):
    with open(
        target_path(context, "run_results.json")
    ) as stream:
        return json.load(stream)["results"]


def _get_fal_results_file_name(context):
    target = target_path(context)
    pattern = re.compile("fal_results_*.\\.json")
    target_files = list(os.walk(target))[0][2]
    return list(filter(lambda file: pattern.match(file), target_files))


def _set_profiles_dir(context) -> Path:
    # TODO: Ideally this needs to change in just one place
    available_profiles = [
        "postgres",
        "bigquery",
        "redshift",
        "snowflake",
        "duckdb",
        "athena",
        "fal"
    ]
    if "profile" in context.config.userdata:
        profile = context.config.userdata["profile"]
        if profile not in available_profiles:
            raise Exception(f"Profile {profile} is not supported")
        raw_path = reduce(os.path.join, [os.getcwd(), "profiles", profile])
        path = Path(raw_path).absolute()
    elif "profiles_dir" in context:
        path = Path(context.profiles_dir).absolute()
    else:
        # Use postgres profile
        path = Path(context.base_dir).parent.absolute()

    os.environ["profiles_dir"] = str(path)
    return path
