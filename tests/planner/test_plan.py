import networkx as nx
import pytest
from tests.planner.data import GRAPHS

from fal.planner.plan import plan_graph


@pytest.mark.parametrize("graph_info", GRAPHS)
def test_planner_sub_graphs(graph_info):
    graph = graph_info["graph"]
    new_graph = plan_graph(graph)

    subgraphs = [
        maybe_subgraph
        for maybe_subgraph in new_graph.nodes
        if isinstance(maybe_subgraph, nx.DiGraph)
    ]

    assert {frozenset(subgraph.nodes) for subgraph in subgraphs} == graph_info["subgraphs"]
