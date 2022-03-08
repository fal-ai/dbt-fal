from statistics import mode
import networkx as nx

from unittest.mock import MagicMock
from dbt.contracts.graph.parsed import ParsedModelNode
from unittest.mock import patch
from fal import DbtModel
from fal.node_graph import _add_after_scripts


@patch("dbt.contracts.graph.parsed.ParsedModelNode")
def test_bla(parsed_node):
    graph = nx.DiGraph()
    node_lookup = {}
    modelA = _create_mock_model(parsed_node, "modelA", ["scriptA", "scriptB"])
    graph, node_lookup = _add_after_scripts(
        modelA, "model.test", "fal", "/dir", graph, node_lookup
    )
    assert node_lookup.keys ==


def _create_mock_model(model_node_class, name, script_paths):
    parsedNodeMockInstance = model_node_class()
    parsedNodeMockInstance.name = name
    model = DbtModel(parsedNodeMockInstance)
    model.get_script_paths = MagicMock(return_value=script_paths)
    return model
