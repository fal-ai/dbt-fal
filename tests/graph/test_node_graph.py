import networkx as nx

from unittest.mock import MagicMock, PropertyMock
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
        ["script.modelA.AFTER.scriptA.py", "script.modelA.AFTER.scriptB.py"],
    )
    assert_contains_all(
        list(graph.successors("model.modelA")),
        ["script.modelA.AFTER.scriptA.py", "script.modelA.AFTER.scriptB.py"],
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
        ["script.modelA.BEFORE.scriptA.py", "script.modelA.BEFORE.scriptB.py"],
    )

    assert_contains_all(
        list(graph.predecessors("model.modelA")),
        ["script.modelA.BEFORE.scriptA.py", "script.modelA.BEFORE.scriptB.py"],
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
    # modelX = create_mock_model(parsed_node, "modelx", [], ["modelB", "modelC"])
    # modelY = create_mock_model(parsed_node, "modely", [], ["modelB"])
    # modelZ = create_mock_model(parsed_node, "modelz", [], ["modelC"])
    instance = parsed_node()
    modelA = create_mock_model(instance, "modelA", ["scriptA.py", "scriptB.py"], [])
    modelB = create_mock_model(
        parsed_node(), "modelB", ["scriptB.py"], ["model.modelA"]
    )
    modelC = create_mock_model(
        parsed_node(), "modelC", ["scriptC.py"], ["model.modelA"]
    )

    type(instance).depends_on_nodes = PropertyMock(
        side_effect=[[], ["model.modelA"], ["model.modelA"]]
    )

    fal_dbt_instance = fal_dbt_class("/dir", "/profiles")
    fal_dbt_instance.list_models = MagicMock(return_value=[modelA, modelB, modelC])
    fal_dbt_instance.keyword = "fal"

    node_graph = NodeGraph.from_fal_dbt(fal_dbt_instance)

    clean_subgraph = node_graph.generate_safe_subgraphs()
    print(clean_subgraph)
    # assert_contains_all(
    #     list(node_graph.node_lookup.keys()),
    #     [
    #         "model.modelA",
    #         "model.modelB",
    #         "model.modelC",
    #         "script.modelA.AFTER.scriptA.py",
    #         "script.modelA.AFTER.scriptB.py",
    #         "script.modelB.AFTER.scriptB.py",
    #         "script.modelC.AFTER.scriptC.py",
    #     ],
    # )

    # assert_contains_all(
    #     node_graph.get_descendants("model.modelA"),
    #     ["script.modelA.AFTER.scriptB.py", "script.modelA.AFTER.scriptA.py"],
    # )

    # assert_contains_all(
    #     node_graph.get_descendants("model.modelB"),
    #     [
    #         "model.modelA",
    #         "script.modelA.AFTER.scriptB.py",
    #         "script.modelA.AFTER.scriptA.py",
    #         "script.modelB.AFTER.scriptB.py",
    #     ],
    # )

    # assert_contains_all(
    #     node_graph.get_descendants("model.modelC"),
    #     [
    #         "model.modelA",
    #         "script.modelA.AFTER.scriptB.py",
    #         "script.modelA.AFTER.scriptA.py",
    #         "script.modelC.AFTER.scriptC.py",
    #     ],
    # )
