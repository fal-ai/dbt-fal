from typing import List, cast, Union
from fal.run_scripts import raise_for_run_results_failures, run_scripts
from fal.cli.dbt_runner import dbt_run, raise_for_dbt_run_errors
from fal.cli.fal_runner import create_fal_dbt
from fal.cli.selectors import ExecutionPlan
from fal.fal_script import FalScript
from fal.node_graph import FalFlowNode, NodeGraph, ScriptNode
from faldbt.project import FalDbt
import argparse
from fal.telemetry import telemetry


def fal_flow_run(parsed: argparse.Namespace):
    fal_dbt = create_fal_dbt(parsed)
    node_graph = NodeGraph.from_fal_dbt(fal_dbt)
    execution_plan = ExecutionPlan.create_plan_from_graph(parsed, node_graph, fal_dbt)
    main_graph = NodeGraph.from_fal_dbt(fal_dbt)
    sub_graphs = [main_graph]
    if parsed.experimental_flow:
        sub_graphs = main_graph.generate_sub_graphs()

    if len(sub_graphs) > 1:
        telemetry.log_call("fal_in_the_middle")

    for (index, node_graph) in enumerate(sub_graphs):
        if index > 0:
            # we want to run all the nodes if we are not running the first subgraph
            parsed.select = None
        _run_sub_graph(index, parsed, node_graph, execution_plan, fal_dbt)


def _run_sub_graph(
    index: int,
    parsed: argparse.Namespace,
    node_graph: NodeGraph,
    plan: ExecutionPlan,
    fal_dbt: FalDbt,
):
    nodes = list(node_graph.graph.nodes())

    # we still want to seperate nodes as before/dbt/after and we can rely on the plan
    # to do that
    before_scripts = _id_to_fal_scripts(
        node_graph, list(filter(lambda node: node in nodes, plan.before_scripts))
    )

    dbt_nodes = list(filter(lambda node: node in nodes, plan.dbt_models))

    after_scripts = _id_to_fal_scripts(
        node_graph, list(filter(lambda node: node in nodes, plan.after_scripts))
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

    if len(after_scripts) != 0:
        results = run_scripts(after_scripts, fal_dbt)
        raise_for_run_results_failures(after_scripts, results)


def _id_to_fal_scripts(node_graph: NodeGraph, id_list: List[str]) -> List[FalScript]:
    return _flow_node_to_fal_scripts(
        list(
            map(
                lambda id: node_graph.get_node(id),
                id_list,
            )
        )
    )


def _flow_node_to_fal_scripts(list: List[Union[FalFlowNode, None]]) -> List[FalScript]:
    new_list: List[FalScript] = []
    for item in list:
        if item != None and isinstance(item, ScriptNode):
            new_list.append(cast(ScriptNode, item).script)
    return new_list


def _unique_id_to_model_name(unique_id: str):
    split_list = unique_id.split(".")
    # if its a unique id 'model.fal_test.model_with_before_scripts'
    return split_list[len(split_list) - 1]


def _unique_ids_to_model_names(id_list: List[str]):
    return list(map(lambda id: _unique_id_to_model_name(id), id_list))
