from fal.cli.selectors import ExecutionPlan
from fal.node_graph import NodeGraph
import networkx as nx
from argparse import Namespace
from utils import assert_contains_all


def test_execution_plan_only_dbt():
    ids_to_execute = ["modelA", "modelB"]
    plan = ExecutionPlan(ids_to_execute)
    assert plan.after_scripts == []
    assert plan.before_scripts == []
    assert plan.dbt_models == ["modelA", "modelB"]


def test_execution_plan_all_empty():
    ids_to_execute = []
    plan = ExecutionPlan(ids_to_execute)
    assert plan.after_scripts == []
    assert plan.before_scripts == []
    assert plan.dbt_models == []


def test_execution_plan_mixed():
    ids_to_execute = [
        "modelA",
        "script.modelA.AFTER.scriptName.py",
        "script.modelA.BEFORE.scriptName.py",
        "script.modelB.BEFORE.scriptName.py",
        "script.modelB.BEFORE.scriptNameA.py",
        "script.modelB.AFTER.scriptNameB.py",
    ]
    plan = ExecutionPlan(ids_to_execute)
    assert_contains_all(
        plan.after_scripts,
        [
            "script.modelA.AFTER.scriptName.py",
            "script.modelB.AFTER.scriptNameB.py",
        ],
    )
    assert_contains_all(
        plan.before_scripts,
        [
            "script.modelA.BEFORE.scriptName.py",
            "script.modelB.BEFORE.scriptName.py",
            "script.modelB.BEFORE.scriptNameA.py",
        ],
    )
    assert plan.dbt_models == ["modelA"]


def test_create_plan_before_downstream():
    parsed = Namespace(select=["scriptC.py+"])
    graph = _create_test_graph()

    execution_plan = ExecutionPlan.create_plan_from_graph(parsed, graph)

    assert execution_plan.before_scripts == ["script.model.BEFORE.scriptC.py"]
    assert execution_plan.dbt_models == ["model.modelA"]
    assert_contains_all(
        execution_plan.after_scripts,
        [
            "script.model.AFTER.scriptA.py",
            "script.model.AFTER.scriptB.py",
        ],
    )


def test_create_plan_start_model_downstream():
    parsed = Namespace(select=["modelA+"])
    graph = _create_test_graph()

    execution_plan = ExecutionPlan.create_plan_from_graph(parsed, graph)

    assert execution_plan.before_scripts == []
    assert execution_plan.dbt_models == ["model.modelA"]
    assert_contains_all(
        execution_plan.after_scripts,
        [
            "script.model.AFTER.scriptA.py",
            "script.model.AFTER.scriptB.py",
        ],
    )


def test_create_plan_start_model_upstream():
    parsed = Namespace(select=["+modelA"])
    graph = _create_test_graph()

    execution_plan = ExecutionPlan.create_plan_from_graph(parsed, graph)

    assert_contains_all(
        execution_plan.before_scripts,
        [
            "script.model.BEFORE.scriptC.py",
            "script.model.BEFORE.scriptD.py",
        ],
    )
    assert execution_plan.dbt_models == ["model.modelA"]
    assert execution_plan.after_scripts == []


def test_create_plan_start_model_upstream_and_downstream():
    parsed = Namespace(select=["+modelA+"])
    graph = _create_test_graph()

    execution_plan = ExecutionPlan.create_plan_from_graph(parsed, graph)

    assert_contains_all(
        execution_plan.before_scripts,
        [
            "script.model.BEFORE.scriptC.py",
            "script.model.BEFORE.scriptD.py",
        ],
    )
    assert execution_plan.dbt_models == ["model.modelA"]
    assert_contains_all(
        execution_plan.after_scripts,
        [
            "script.model.AFTER.scriptA.py",
            "script.model.AFTER.scriptB.py",
        ],
    )


def test_create_plan_start_after_downstream():
    parsed = Namespace(select=["scriptA.py+"])
    graph = _create_test_graph()

    execution_plan = ExecutionPlan.create_plan_from_graph(parsed, graph)

    assert execution_plan.before_scripts == []
    assert execution_plan.dbt_models == []
    assert execution_plan.after_scripts == [
        "script.model.AFTER.scriptA.py",
    ]


def test_create_plan_no_graph_selectors():
    parsed = Namespace(select=["modelA", "modelB"])
    graph = _create_test_graph()

    execution_plan = ExecutionPlan.create_plan_from_graph(parsed, graph)

    assert execution_plan.before_scripts == []
    assert_contains_all(
        execution_plan.dbt_models,
        [
            "model.modelA",
            "model.modelB",
        ],
    )
    assert execution_plan.after_scripts == []


def _create_test_graph():
    scriptA = "script.model.AFTER.scriptA.py"
    scriptB = "script.model.AFTER.scriptB.py"
    scriptC = "script.model.BEFORE.scriptC.py"
    scriptD = "script.model.BEFORE.scriptD.py"

    modelA = "model.modelA"
    modelB = "model.modelB"

    graph = nx.DiGraph()

    graph.add_node(scriptA)
    graph.add_node(scriptB)
    graph.add_node(scriptC)
    graph.add_node(scriptD)
    graph.add_node(modelA)
    graph.add_node(modelB)

    graph.add_edge(modelA, scriptA)
    graph.add_edge(modelA, scriptB)
    graph.add_edge(scriptC, modelA)
    graph.add_edge(scriptD, modelA)

    return NodeGraph(graph, {})
