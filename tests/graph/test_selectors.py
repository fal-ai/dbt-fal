from fal.cli.selectors import ExecutionPlan
from fal.node_graph import NodeGraph
import networkx as nx
from argparse import Namespace
from utils import assert_contains_only
from unittest.mock import MagicMock

PROJECT_NAME = "test_project"


def test_execution_plan_only_dbt():
    ids_to_execute = ["modelA", "modelB"]
    plan = ExecutionPlan(ids_to_execute, PROJECT_NAME)
    assert plan.after_scripts == []
    assert plan.before_scripts == []
    assert plan.dbt_models == ["modelA", "modelB"]


def test_execution_plan_all_empty():
    ids_to_execute = []
    plan = ExecutionPlan(ids_to_execute, PROJECT_NAME)
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
    plan = ExecutionPlan(ids_to_execute, PROJECT_NAME)
    assert_contains_only(
        plan.after_scripts,
        [
            "script.modelA.AFTER.scriptName.py",
            "script.modelB.AFTER.scriptNameB.py",
        ],
    )
    assert_contains_only(
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

    execution_plan = ExecutionPlan.create_plan_from_graph(
        parsed, graph, MagicMock(project_name=PROJECT_NAME)
    )

    assert execution_plan.before_scripts == ["script.model.BEFORE.scriptC.py"]
    assert execution_plan.dbt_models == ["model.test_project.modelA"]
    assert_contains_only(
        execution_plan.after_scripts,
        [
            "script.model.AFTER.scriptA.py",
            "script.model.AFTER.scriptB.py",
        ],
    )


def test_create_plan_start_model_downstream():
    parsed = Namespace(select=["modelA+"])
    graph = _create_test_graph()

    execution_plan = ExecutionPlan.create_plan_from_graph(
        parsed, graph, MagicMock(project_name=PROJECT_NAME)
    )

    assert execution_plan.before_scripts == []
    assert execution_plan.dbt_models == ["model.test_project.modelA"]
    assert_contains_only(
        execution_plan.after_scripts,
        [
            "script.model.AFTER.scriptA.py",
            "script.model.AFTER.scriptB.py",
        ],
    )


def test_create_plan_start_model_upstream():
    parsed = Namespace(select=["+modelA"])
    graph = _create_test_graph()

    execution_plan = ExecutionPlan.create_plan_from_graph(
        parsed, graph, MagicMock(project_name=PROJECT_NAME)
    )

    assert_contains_only(
        execution_plan.before_scripts,
        [
            "script.model.BEFORE.scriptC.py",
            "script.model.BEFORE.scriptD.py",
        ],
    )
    assert execution_plan.dbt_models == ["model.test_project.modelA"]
    assert execution_plan.after_scripts == []


def test_create_plan_large_graph_model_levels():
    def _model(s: str) -> str:
        return f"model.test_project.{s}"

    def _after_script_for_model(model: str) -> str:
        return f"script.{model}.AFTER.script.py"

    digraph = nx.DiGraph()

    for n in range(100):
        modeln_name = f"model{n}"
        modeln = _model(modeln_name)

        digraph.add_edge(modeln, _after_script_for_model(modeln_name))

        modelnext = f"model.test_project.model{n+1}"
        digraph.add_edge(modeln, modelnext)

        for m in range(10):  # Avoid cycles with these ranges
            modelm = _model(f"model{n}_{m}")
            digraph.add_edge(modeln, modelm)

    graph = NodeGraph(digraph, {})

    parsed = Namespace(select=["model0+70"])

    execution_plan = ExecutionPlan.create_plan_from_graph(
        parsed, graph, MagicMock(project_name=PROJECT_NAME)
    )

    assert execution_plan.before_scripts == []
    assert_contains_only(
        execution_plan.dbt_models,
        # model0, model1, ..., model70
        [_model(f"model{n}") for n in range(71)]
        # model0_0, model0_1, ..., model0_9, model1_0, ..., model69_0, ..., model69_9
        # not the children of model70, it ends in model70
        + [_model(f"model{n}_{m}") for m in range(10) for n in range(70)],
    )
    assert_contains_only(
        execution_plan.after_scripts,
        # after script for model0, model1, ..., model69
        # not model70 because that is one level too far
        [_after_script_for_model(f"model{n}") for n in range(70)],
    )


def test_create_plan_start_model_upstream_and_downstream():
    parsed = Namespace(select=["+modelA+"])
    graph = _create_test_graph()

    execution_plan = ExecutionPlan.create_plan_from_graph(
        parsed, graph, MagicMock(project_name=PROJECT_NAME)
    )

    assert_contains_only(
        execution_plan.before_scripts,
        [
            "script.model.BEFORE.scriptC.py",
            "script.model.BEFORE.scriptD.py",
        ],
    )
    assert execution_plan.dbt_models == ["model.test_project.modelA"]
    assert_contains_only(
        execution_plan.after_scripts,
        [
            "script.model.AFTER.scriptA.py",
            "script.model.AFTER.scriptB.py",
        ],
    )


def test_create_plan_start_after_downstream():
    parsed = Namespace(select=["scriptA.py+"])
    graph = _create_test_graph()

    execution_plan = ExecutionPlan.create_plan_from_graph(
        parsed, graph, MagicMock(project_name=PROJECT_NAME)
    )

    assert execution_plan.before_scripts == []
    assert execution_plan.dbt_models == []
    assert execution_plan.after_scripts == [
        "script.model.AFTER.scriptA.py",
    ]


def test_create_plan_no_graph_selectors():
    parsed = Namespace(select=["modelA", "modelB"])
    graph = _create_test_graph()

    execution_plan = ExecutionPlan.create_plan_from_graph(
        parsed, graph, MagicMock(project_name=PROJECT_NAME)
    )

    assert execution_plan.before_scripts == []
    assert_contains_only(
        execution_plan.dbt_models,
        [
            "model.test_project.modelA",
            "model.test_project.modelB",
        ],
    )
    assert execution_plan.after_scripts == []


def _create_test_graph():
    scriptA = "script.model.AFTER.scriptA.py"
    scriptB = "script.model.AFTER.scriptB.py"
    scriptC = "script.model.BEFORE.scriptC.py"
    scriptD = "script.model.BEFORE.scriptD.py"

    modelA = "model.test_project.modelA"
    modelB = "model.test_project.modelB"

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
