import pytest
import networkx as nx

from unittest.mock import MagicMock
from unittest.mock import patch
from utils import assert_contains_only, create_mock_model

from fal.dbt.node_graph import NodeGraph, _add_after_scripts, _add_before_scripts
import fal.dbt.integration.version as version



# TODO: fix mocking for 1.5 and remove skips
@pytest.mark.skipif(version.is_version_plus("1.4.0"), reason="mocking <1.4 modules")
@patch("dbt.contracts.graph.parsed.ParsedModelNode")
@patch("fal.dbt.FalDbt")
def test_add_after_scripts(parsed_node, fal_dbt_class):
    graph = nx.DiGraph()
    node_lookup = {}
    modelA = create_mock_model(parsed_node, "modelA", ["scriptA.py", "scriptB.py"], [])

    fal_dbt_instance = fal_dbt_class("/dir", "/profiles")
    fal_dbt_instance.scripts_dir = "/dir"
    graph, node_lookup = _add_after_scripts(
        modelA, "model.modelA", fal_dbt_instance, graph, node_lookup
    )

    assert_contains_only(
        list(node_lookup.keys()),
        ["script.modelA.AFTER.scriptA.py", "script.modelA.AFTER.scriptB.py"],
    )
    assert_contains_only(
        list(graph.successors("model.modelA")),
        ["script.modelA.AFTER.scriptA.py", "script.modelA.AFTER.scriptB.py"],
    )


@pytest.mark.skipif(version.is_version_plus("1.4.0"), reason="mocking <1.4 modules")
@patch("dbt.contracts.graph.parsed.ParsedModelNode")
@patch("fal.dbt.FalDbt")
def test_add_before_scripts(parsed_node, fal_dbt_class):
    graph = nx.DiGraph()
    node_lookup = {}
    modelA = create_mock_model(
        parsed_node, "modelA", [], [], before_script_paths=["scriptA.py", "scriptB.py"]
    )

    fal_dbt_instance = fal_dbt_class("/dir", "/profiles")
    fal_dbt_instance.scripts_dir = "/dir"
    graph, node_lookup = _add_before_scripts(
        modelA, "model.modelA", fal_dbt_instance, graph, node_lookup
    )

    assert_contains_only(
        list(node_lookup.keys()),
        ["script.modelA.BEFORE.scriptA.py", "script.modelA.BEFORE.scriptB.py"],
    )

    assert_contains_only(
        list(graph.predecessors("model.modelA")),
        ["script.modelA.BEFORE.scriptA.py", "script.modelA.BEFORE.scriptB.py"],
    )


@patch("fal.dbt.FalDbt")
def test_empty_fal_dbt(fal_dbt_class):
    fal_dbt_instance = fal_dbt_class("/dir", "/profiles")
    fal_dbt_instance.scripts_dir = "/dir"
    fal_dbt_instance.list_models = MagicMock(return_value=[])
    node_graph = NodeGraph.from_fal_dbt(fal_dbt_instance)

    assert list(node_graph.node_lookup.keys()) == []


@pytest.mark.skipif(version.is_version_plus("1.4.0"), reason="mocking <1.4 modules")
@patch("dbt.contracts.graph.parsed.ParsedModelNode")
@patch("fal.dbt.FalDbt")
def test_create_with_fal_dbt(parsed_node, fal_dbt_class):
    modelA = create_mock_model(parsed_node, "modelA", ["scriptA.py", "scriptB.py"], [])
    modelB = create_mock_model(parsed_node, "modelB", ["scriptB.py"], ["model.modelA"])
    modelC = create_mock_model(
        parsed_node, "modelC", ["scriptC.py"], ["model.modelA", "model.modelB"]
    )
    fal_dbt_instance = fal_dbt_class("/dir", "/profiles")
    fal_dbt_instance.scripts_dir = "/dir"
    fal_dbt_instance.list_models = MagicMock(return_value=[modelA, modelB, modelC])

    node_graph = NodeGraph.from_fal_dbt(fal_dbt_instance)

    assert_contains_only(
        list(node_graph.node_lookup.keys()),
        [
            "model.modelA",
            "model.modelB",
            "model.modelC",
            "script.modelA.AFTER.scriptA.py",
            "script.modelA.AFTER.scriptB.py",
            "script.modelB.AFTER.scriptB.py",
            "script.modelC.AFTER.scriptC.py",
        ],
    )

    assert_contains_only(
        node_graph.get_descendants("model.modelA"),
        [
            "model.modelC",
            "script.modelA.AFTER.scriptB.py",
            "script.modelC.AFTER.scriptC.py",
            "script.modelA.AFTER.scriptA.py",
            "model.modelB",
            "script.modelB.AFTER.scriptB.py",
        ],
    )

    assert_contains_only(
        node_graph.get_descendants("model.modelB"),
        [
            "script.modelB.AFTER.scriptB.py",
            "model.modelC",
            "script.modelC.AFTER.scriptC.py",
        ],
    )

    assert_contains_only(
        node_graph.get_descendants("model.modelC"), ["script.modelC.AFTER.scriptC.py"]
    )
