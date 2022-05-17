from functools import reduce
import json
import os
from pathlib import Path
from typing import Dict, List, cast, Union

from fal.run_scripts import raise_for_run_results_failures, run_scripts
from fal.cli.dbt_runner import dbt_run, raise_for_dbt_run_errors
from fal.cli.fal_runner import create_fal_dbt
from fal.cli.selectors import ExecutionPlan
from fal.cli.model_generator import generate_python_dbt_models
from fal.fal_script import FalScript
from fal.node_graph import DbtModelNode, FalFlowNode, NodeGraph, ScriptNode
from faldbt.project import FalDbt
import argparse
from fal.telemetry import telemetry

RUN_RESULTS_FILE_NAME = "run_results.json"
RUN_RESULTS_KEY = "results"


def fal_flow_run(parsed: argparse.Namespace):
    generated_models: Dict[str, Path] = {}
    if parsed.experimental_python_models:
        telemetry.log_call("experimental_python_models")
        generated_models = generate_python_dbt_models(parsed.project_dir)

    fal_dbt = create_fal_dbt(parsed, generated_models)

    node_graph = NodeGraph.from_fal_dbt(fal_dbt)
    execution_plan = ExecutionPlan.create_plan_from_graph(parsed, node_graph, fal_dbt)
    main_graph = NodeGraph.from_fal_dbt(fal_dbt)
    sub_graphs = [main_graph]
    if parsed.experimental_flow or parsed.experimental_python_models:
        sub_graphs = main_graph.generate_sub_graphs()

    if len(sub_graphs) > 1:
        telemetry.log_call("fal_in_the_middle")

    for index, node_graph in enumerate(sub_graphs):
        if index > 0:
            # we want to run all the nodes if we are not running the first subgraph
            parsed.select = None
        _run_sub_graph(index, parsed, node_graph, execution_plan, fal_dbt)

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

    if len(before_scripts) != 0:
        results = run_scripts(before_scripts, fal_dbt)
        raise_for_run_results_failures(before_scripts, results)

    if len(dbt_nodes) != 0:
        output = dbt_run(
            parsed,
            _unique_ids_to_model_names(dbt_nodes),
            fal_dbt.target_path,
            index,
        )
        raise_for_dbt_run_errors(output)

        fal_nodes = []
        for n in dbt_nodes:
            mnode = cast(DbtModelNode, node_graph.get_node(n))
            if mnode is not None and mnode.model.python_model is not None:
                fal_nodes.append(n)
        if len(fal_nodes) != 0:
            results = run_scripts(
                _id_to_fal_scripts(node_graph, fal_dbt, fal_nodes), fal_dbt
            )
            raise_for_run_results_failures(after_scripts, results)

    if len(after_scripts) != 0:
        results = run_scripts(after_scripts, fal_dbt)
        raise_for_run_results_failures(after_scripts, results)


def _id_to_fal_scripts(
    node_graph: NodeGraph, fal_dbt: FalDbt, id_list: List[str]
) -> List[FalScript]:
    return _flow_node_to_fal_scripts(
        fal_dbt,
        list(
            map(
                lambda id: node_graph.get_node(id),
                id_list,
            )
        ),
    )


def _flow_node_to_fal_scripts(
    fal_dbt: FalDbt, list: List[Union[FalFlowNode, None]]
) -> List[FalScript]:
    new_list: List[FalScript] = []
    for item in list:
        if item is not None and isinstance(item, ScriptNode):
            new_list.append(cast(ScriptNode, item).script)
        elif item is not None and isinstance(item, DbtModelNode):
            new_list.append(FalScript.model_script(fal_dbt, item.model))
    return new_list


def _unique_id_to_model_name(unique_id: str) -> str:
    split_list = unique_id.split(".")
    # if its a unique id 'model.fal_test.model_with_before_scripts'
    return split_list[len(split_list) - 1]


def _unique_ids_to_model_names(id_list: List[str]) -> List[str]:
    return list(map(lambda id: _unique_id_to_model_name(id), id_list))


def _combine_fal_run_results(target_path):
    result_files = _get_fal_run_results(target_path)
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
