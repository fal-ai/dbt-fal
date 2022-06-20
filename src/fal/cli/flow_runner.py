from functools import reduce
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, cast, Union, Tuple, Any, Iterator

from fal.run_scripts import raise_for_run_results_failures, run_scripts
from fal.cli.dbt_runner import dbt_run, raise_for_dbt_run_errors
from fal.cli.fal_runner import create_fal_dbt
from fal.cli.selectors import ExecutionPlan
from fal.cli.model_generator import generate_python_dbt_models
from fal.fal_script import FalScript
from fal.node_graph import DbtModelNode, FalFlowNode, NodeGraph, ScriptNode
from faldbt.project import FalDbt, NodeStatus
import argparse
from fal.telemetry import telemetry

RUN_RESULTS_FILE_NAME = "run_results.json"
RUN_RESULTS_KEY = "results"


def _is_experimental_models_enabled(parsed: argparse.Namespace):
    """Whether experimental models are enabled or not."""
    return parsed.experimental_python_models or parsed.experimental_threads is not None


def run_serial(
    fal_dbt: FalDbt,
    parsed: argparse.Namespace,
    node_graph: NodeGraph,
) -> None:
    main_graph = NodeGraph.from_fal_dbt(fal_dbt)
    if parsed.experimental_flow or _is_experimental_models_enabled(parsed):
        sub_graphs = main_graph.generate_sub_graphs()
    else:
        sub_graphs = [main_graph]

    if len(sub_graphs) > 1:
        telemetry.log_call("fal_in_the_middle")

    execution_plan = ExecutionPlan.create_plan_from_graph(parsed, node_graph, fal_dbt)
    for index, node_graph in enumerate(sub_graphs):
        if index > 0:
            # we want to run all the nodes if we are not running the first subgraph
            parsed.select = None
        _run_sub_graph(index, parsed, node_graph, execution_plan, fal_dbt)


def run_threaded(
    fal_dbt: FalDbt,
    parsed: argparse.Namespace,
    node_graph: NodeGraph,
) -> None:
    if parsed.experimental_threads <= 0:
        raise ValueError("Number of specified threads must be greater than 0")

    raise NotImplementedError("This feature is not available at the moment.")


def fal_flow_run(parsed: argparse.Namespace):
    generated_models: Dict[str, Path] = {}
    if _is_experimental_models_enabled(parsed):
        telemetry.log_call("experimental_python_models")
        generated_models = generate_python_dbt_models(parsed.project_dir)

    fal_dbt = create_fal_dbt(parsed, generated_models)
    _mark_dbt_nodes_status(fal_dbt, NodeStatus.Skipped)

    node_graph = NodeGraph.from_fal_dbt(fal_dbt)
    if parsed.experimental_threads is not None:
        telemetry.log_call("experimental_threads")
        run_threaded(fal_dbt=fal_dbt, parsed=parsed, node_graph=node_graph)
    else:
        run_serial(fal_dbt=fal_dbt, parsed=parsed, node_graph=node_graph)

    # each dbt run creates its own run_results file, here we are combining
    # these files in a single run_results file that fits dbt file format
    _combine_fal_run_results(fal_dbt.target_path)


def _run_sub_graph(
    index: int,
    parsed: argparse.Namespace,
    node_graph: NodeGraph,
    plan: ExecutionPlan,
    fal_dbt: FalDbt,
):
    nodes = list(node_graph.graph.nodes())

    # we still want to seperate nodes as before/model/after and we can rely on the plan
    # to do that
    before_scripts = _id_to_fal_scripts(
        node_graph,
        fal_dbt,
        list(filter(lambda node: node in nodes, plan.before_scripts)),
    )

    dbt_nodes = list(filter(lambda node: node in nodes, plan.dbt_models))

    after_scripts = _id_to_fal_scripts(
        node_graph,
        fal_dbt,
        list(filter(lambda node: node in nodes, plan.after_scripts)),
    )

    if before_scripts:
        results = run_scripts(before_scripts, fal_dbt)
        raise_for_run_results_failures(before_scripts, results)

    if dbt_nodes:
        output = dbt_run(
            parsed,
            _unique_ids_to_model_names(dbt_nodes),
            fal_dbt.target_path,
            index,
        )

        for node, status in _map_cli_output_model_statuses(output.run_results):
            _mark_dbt_nodes_status(fal_dbt, status, node)

        fal_nodes = []
        post_hooks = []
        for n in dbt_nodes:
            mnode = cast(DbtModelNode, node_graph.get_node(n))
            if mnode is not None and mnode.model.python_model is not None:
                fal_nodes.append(n)

            if mnode is not None:
                model_hooks = [
                    FalScript(fal_dbt, mnode.model, path, True)
                    for path in mnode.model.get_post_hook_paths()
                ]
                post_hooks.extend(model_hooks)

        fal_nodes_results = {}
        if fal_nodes:
            python_node_scripts = _id_to_fal_scripts(node_graph, fal_dbt, fal_nodes)
            results = run_scripts(python_node_scripts, fal_dbt)
            # We need to hold on to results until after we run post-hooks
            fal_nodes_results = {"scripts": python_node_scripts, "results": results}

            # Update dbt node status of Python nodes
            for script, success in zip(python_node_scripts, results):
                status = NodeStatus.Success if success else NodeStatus.Error
                _mark_dbt_nodes_status(
                    fal_dbt,
                    status,
                    script.model.unique_id,
                )

        if post_hooks:
            results = run_scripts(post_hooks, fal_dbt)
            raise_for_run_results_failures(post_hooks, results)

        if fal_nodes_results:
            raise_for_run_results_failures(**fal_nodes_results)

        raise_for_dbt_run_errors(output)

    if after_scripts:
        results = run_scripts(after_scripts, fal_dbt)
        raise_for_run_results_failures(after_scripts, results)


def _mark_dbt_nodes_status(
    fal_dbt: FalDbt, status: NodeStatus, dbt_node: Optional[str] = None
):
    for model in fal_dbt.models:
        if dbt_node is not None:
            if model.unique_id == dbt_node:
                model.status = status
        else:
            model.status = status


def _map_cli_output_model_statuses(
    run_results: Dict[Any, Any]
) -> Iterator[Tuple[str, NodeStatus]]:
    if not isinstance(run_results.get("results"), list):
        raise Exception("Could not read dbt run results")

    for result in run_results["results"]:
        if not result.get("unique_id") or not result.get("status"):
            continue

        yield result["unique_id"], NodeStatus(result["status"])


def _id_to_fal_scripts(
    node_graph: NodeGraph, fal_dbt: FalDbt, id_list: List[str]
) -> List[FalScript]:
    return [node_to_script(node_graph.get_node(id_), fal_dbt) for id_ in id_list]


def node_to_script(node: Union[FalFlowNode, None], fal_dbt: FalDbt) -> FalScript:
    """Convert dbt node into a FalScript."""
    if node is not None and isinstance(node, ScriptNode):
        return cast(ScriptNode, node).script
    elif node is not None and isinstance(node, DbtModelNode):
        return FalScript.model_script(fal_dbt, node.model)
    else:
        raise Exception(f"Cannot convert node to script. Node: {node}")


def _unique_id_to_model_name(unique_id: str) -> str:
    split_list = unique_id.split(".")
    # if its a unique id 'model.fal_test.model_with_before_scripts'
    return split_list[len(split_list) - 1]


def _unique_ids_to_model_names(id_list: List[str]) -> List[str]:
    return list(map(lambda id: _unique_id_to_model_name(id), id_list))


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
