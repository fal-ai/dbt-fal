import networkx as nx

from unittest.mock import MagicMock
from unittest.mock import patch
from fal.node_graph import NodeGraph, _add_after_scripts, _add_before_scripts
from utils import assert_contains_all, create_mock_model


@patch("dbt.contracts.graph.parsed.ParsedModelNode")
def test_add_after_scripts(parsed_node):
    graph = nx.DiGraph()
    node_lookup = {}
    modelA = create_mock_model(parsed_node, "modelA", ["scriptA.py", "scriptB.py"])

    graph, node_lookup = _add_after_scripts(
        modelA, "model.modelA", "fal", "/dir", graph, node_lookup
    )

    assert_contains_all(
        list(node_lookup.keys()),
        ["script.modelA.scriptA.py", "script.modelA.scriptB.py"],
    )
    assert_contains_all(
        list(graph.successors("model.modelA")),
        ["script.modelA.scriptA.py", "script.modelA.scriptB.py"],
    )


@patch("dbt.contracts.graph.parsed.ParsedModelNode")
def test_add_before_scripts(parsed_node):
    graph = nx.DiGraph()
    node_lookup = {}
    modelA = create_mock_model(parsed_node, "modelA", ["scriptA.py", "scriptB.py"])

    graph, node_lookup = _add_before_scripts(
        modelA, "model.modelA", "fal", "/dir", graph, node_lookup
    )

    assert_contains_all(
        list(node_lookup.keys()),
        ["script.modelA.scriptA.py", "script.modelA.scriptB.py"],
    )

    assert_contains_all(
        list(graph.predecessors("model.modelA")),
        ["script.modelA.scriptA.py", "script.modelA.scriptB.py"],
    )


@patch("fal.FalDbt")
def test_empty_fal_dbt(fal_dbt_class):
    fal_dbt_instance = fal_dbt_class("/dir", "/profiles")
    fal_dbt_instance.list_models = MagicMock(return_value=[])
    fal_dbt_instance.keyword = "fal"
    node_graph = NodeGraph.from_fal_dbt(fal_dbt_instance)

    assert list(node_graph.node_lookup.keys()) == []


@patch("dbt.contracts.graph.parsed.ParsedModelNode")
@patch("fal.FalDbt")
def test_create_with_fal_dbt(parsed_node, fal_dbt_class):
    modelA = create_mock_model(parsed_node, "modelA", ["scriptA.py", "scriptB.py"])
    modelB = create_mock_model(parsed_node, "modelB", ["scriptB.py"], ["model.modelA"])
    modelC = create_mock_model(parsed_node, "modelC", ["scriptC.py"], ["model.modelA"])
    fal_dbt_instance = fal_dbt_class("/dir", "/profiles")
    fal_dbt_instance.list_models = MagicMock(return_value=[modelA, modelB, modelC])
    fal_dbt_instance.keyword = "fal"

    node_graph = NodeGraph.from_fal_dbt(fal_dbt_instance)

    assert_contains_all(
        list(node_graph.node_lookup.keys()),
        [
            "model.modelA",
            "model.modelB",
            "model.modelC",
            "script.modelA.scriptA.py",
            "script.modelA.scriptB.py",
            "script.modelB.scriptB.py",
            "script.modelC.scriptC.py",
        ],
    )

    assert_contains_all(
        node_graph.get_descendants("model.modelA"),
        ["script.modelA.scriptB.py", "script.modelA.scriptA.py"],
    )

    assert_contains_all(
        node_graph.get_descendants("model.modelB"),
        [
            "model.modelA",
            "script.modelA.scriptB.py",
            "script.modelA.scriptA.py",
            "script.modelB.scriptB.py",
        ],
    )

    assert_contains_all(
        node_graph.get_descendants("model.modelC"),
        [
            "model.modelA",
            "script.modelA.scriptB.py",
            "script.modelA.scriptA.py",
            "script.modelC.scriptC.py",
        ],
    )
