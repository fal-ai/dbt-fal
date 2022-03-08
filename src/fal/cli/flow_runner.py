from typing import List, cast, Union
from fal.run_scripts import run_scripts
from fal.cli.dbt_runner import dbt_run
from fal.cli.fal_runner import create_fal_dbt
from fal.cli.selectors import ExecutionPlan
from fal.fal_script import FalScript
from fal.node_graph import FalFlowNode, NodeGraph, ScriptNode
from faldbt.project import FalProject


def fal_flow_run(parsed):
    fal_dbt = create_fal_dbt(parsed)
    project = FalProject(fal_dbt)
    node_graph = NodeGraph.from_fal_dbt(fal_dbt)
    execution_plan = ExecutionPlan.create_plan_from_graph(parsed, node_graph)

    run_scripts(_id_to_fal_scripts(node_graph, execution_plan.before_scripts), project)

    if len(execution_plan.dbt_models) != 0:
        dbt_run(parsed, _unique_ids_to_model_names(execution_plan.dbt_models))

    run_scripts(_id_to_fal_scripts(node_graph, execution_plan.after_scripts), project)


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
