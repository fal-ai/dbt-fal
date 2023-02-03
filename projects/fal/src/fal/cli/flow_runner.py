import json
import copy
from pathlib import Path
from typing import Any, Dict, Optional, cast, Union

from fal.cli.fal_runner import create_fal_dbt
from fal.cli.selectors import ExecutionPlan
from fal.cli.model_generator import generate_python_dbt_models
from fal.fal_script import FalScript
from fal.node_graph import DbtModelNode, FalFlowNode, NodeGraph, ScriptNode
from faldbt.project import FalDbt, NodeStatus
import argparse


DBT_RUN_RESULTS_FILENAME = "run_results.json"
FAL_RUN_RESULTS_FILENAME = "fal_results.json"
RUN_RESULTS_KEY = "results"
ELAPSED_TIME_KEY = "elapsed_time"


def run_threaded(
    fal_dbt: FalDbt,
    parsed: argparse.Namespace,
    node_graph: NodeGraph,
) -> int:
    from fal.planner.plan import (
        OriginGraph,
        FilteredGraph,
        PlannedGraph,
        ScriptConnectedGraph,
    )
    from fal.planner.schedule import schedule_graph
    from fal.planner.executor import parallel_executor

    execution_plan = ExecutionPlan.create_plan_from_graph(parsed, node_graph, fal_dbt)

    origin_graph = OriginGraph(node_graph.graph)
    filtered_graph = FilteredGraph.from_execution_plan(
        origin_graph, execution_plan=execution_plan
    )
    connected_graph = ScriptConnectedGraph.from_filtered_graph(filtered_graph)
    planned_graph = PlannedGraph.from_script_connected_graph(
        connected_graph, enable_chunking=False
    )
    scheduler = schedule_graph(planned_graph.graph, node_graph)
    return parallel_executor(parsed, fal_dbt, scheduler)


def fal_flow_run(parsed: argparse.Namespace) -> int:
    arg_vars = getattr(parsed, "vars", "{}")

    # fal-format Python models
    generated_models = generate_python_dbt_models(parsed.project_dir, arg_vars)

    fal_dbt = create_fal_dbt(parsed, generated_models)
    _mark_dbt_nodes_status(fal_dbt, NodeStatus.Skipped)

    node_graph = NodeGraph.from_fal_dbt(fal_dbt)
    exit_code = run_threaded(fal_dbt=fal_dbt, parsed=parsed, node_graph=node_graph)

    # each dbt run creates its own run_results file, here we are combining
    # these files in a single run_results file that fits dbt file format
    _combine_fal_run_results(fal_dbt.target_path)
    return exit_code


def _mark_dbt_nodes_status(
    fal_dbt: FalDbt, status: NodeStatus, dbt_node: Optional[str] = None
):
    for model in fal_dbt.models:
        if dbt_node is not None:
            if model.unique_id == dbt_node:
                model.status = status
        else:
            model.status = status


def node_to_script(node: Union[FalFlowNode, None], fal_dbt: FalDbt) -> FalScript:
    """Convert dbt node into a FalScript."""
    if node is not None and isinstance(node, ScriptNode):
        return cast(ScriptNode, node).script
    elif node is not None and isinstance(node, DbtModelNode):
        return FalScript.model_script(fal_dbt, node.model)
    else:
        raise Exception(f"Cannot convert node to script. Node: {node}")


def _combine_fal_run_results(target_path: str) -> None:
    target_path = Path(target_path)
    dbt_run_results, fal_run_results = [], []
    for path in target_path.glob("fal_results_*.json"):
        assert path.is_file()

        results = _get_all_result_content(path)

        if "dbt_schema_version" in results.get("metadata", {}):
            dbt_run_results.append(results)
        fal_run_results.append(results)

        # Clear out files as we go.
        path.unlink()

    # Use the last DBT result as the framework for putting
    # the rest of the run results.
    if dbt_run_results:
        result_framework = dbt_run_results[-1]
    else:
        result_framework = {
            "metadata": {},
            "args": {},
            ELAPSED_TIME_KEY: float("nan"),
        }

    for file, results in [
        (DBT_RUN_RESULTS_FILENAME, dbt_run_results),
        (FAL_RUN_RESULTS_FILENAME, fal_run_results),
    ]:
        if not results:
            continue

        combined_results = copy.deepcopy(result_framework)
        combined_results[RUN_RESULTS_KEY] = []
        combined_results[ELAPSED_TIME_KEY] = 0.0

        for result in results:
            combined_results[ELAPSED_TIME_KEY] += result.get(ELAPSED_TIME_KEY, 0)
            combined_results[RUN_RESULTS_KEY].extend(result[RUN_RESULTS_KEY])

        with open(target_path / file, "w") as stream:
            json.dump(combined_results, stream)


def _get_all_result_content(file) -> Dict[str, Any]:
    with open(file) as content:
        return json.load(content)
