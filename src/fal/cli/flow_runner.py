from functools import reduce
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, cast, Union

from fal.cli.fal_runner import create_fal_dbt
from fal.cli.selectors import ExecutionPlan
from fal.cli.model_generator import generate_python_dbt_models, delete_generated_models
from fal.fal_script import FalScript
from fal.node_graph import DbtModelNode, FalFlowNode, NodeGraph, ScriptNode
from faldbt.project import FalDbt, NodeStatus
import argparse

RUN_RESULTS_FILE_NAME = "run_results.json"
RUN_RESULTS_KEY = "results"


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
    generated_models: Dict[str, Path] = {}

    # Python models
    delete_generated_models(parsed.project_dir)
    generated_models = generate_python_dbt_models(parsed.project_dir)

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


def _combine_fal_run_results(target_path):
    result_files = _get_fal_run_results(target_path)

    if not result_files:
        return

    all_results = reduce(
        lambda all, next: all.extend(_get_result_array(next)) or all,
        result_files,
        [],
    )

    last_run_result = _get_all_result_content(result_files[-1])
    last_run_result[RUN_RESULTS_KEY] = all_results
    run_result_file = os.path.join(target_path, RUN_RESULTS_FILE_NAME)

    # remove all run_results_*.json files for the next fal flow run
    _remove_all(result_files)

    with open(run_result_file, "w") as file:
        file.write(json.dumps(last_run_result))
        file.close()


def _remove_all(files: List[str]):
    for file in files:
        os.remove(file) if os.path.exists(file) else None


def _get_all_result_content(file) -> Dict:
    with open(file) as content:
        return json.load(content)


def _get_result_array(file) -> List[dict]:
    return _get_all_result_content(file)[RUN_RESULTS_KEY]


def _get_fal_run_results(target_path) -> List[str]:
    run_result_files = map(
        lambda x: str(x),
        filter(lambda p: p.is_file(), Path(target_path).glob("fal_results_*.json")),
    )
    return list(run_result_files)
