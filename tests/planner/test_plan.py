from cgitb import enable
from platform import node
import networkx as nx
import pytest
from tests.planner.data import GRAPHS, GRAPH_1, GRAPH_2
from tests.planner.utils import to_graph, to_plan, plan_graph

from fal.cli.selectors import ExecutionPlan


@pytest.mark.parametrize("graph_info", GRAPHS)
def test_planner_sub_graphs(graph_info):
    graph = graph_info["graph"]
    new_graph = plan_graph(graph, to_plan(graph))

    subgraphs = [
        maybe_subgraph
        for maybe_subgraph in new_graph.nodes
        if isinstance(maybe_subgraph, nx.DiGraph)
    ]
    assert {frozenset(subgraph.nodes) for subgraph in subgraphs} == graph_info[
        "subgraphs"
    ]


@pytest.mark.parametrize("graph_info", GRAPHS)
def test_planner_disabled_chunking(graph_info):
    graph = graph_info["graph"]
    new_graph = plan_graph(graph, to_plan(graph), enable_chunking=False)

    subgraphs = [
        maybe_subgraph
        for maybe_subgraph in new_graph.nodes
        if isinstance(maybe_subgraph, nx.DiGraph)
    ]
    assert len(subgraphs) == 0


@pytest.mark.parametrize(
    "selected_nodes", [[], ["A", "F"], ["A", "B", "C", "D", "E", "F"]]
)
def test_planner_execution_plan(selected_nodes):
    graph = to_graph(GRAPH_1)
    new_graph = plan_graph(graph, ExecutionPlan(selected_nodes, "<test>"))
    assert list(new_graph.nodes) == selected_nodes


@pytest.mark.parametrize(
    "excluded_nodes, expected_subgraphs",
    [
        ([], {frozenset(("B1", "B2")), frozenset(("E", "F", "G"))}),
        # If we exclude A and B, that means B1 and B2 are now top-level. C is also top-level and
        # all of them are direct dependencies of D (which as some post-hooks), so we'll group them into
        # one.
        (["A", "B"], {frozenset(("B1", "B2", "C", "D")), frozenset(("E", "F", "G"))}),
    ],
)
def test_chunking_with_execution_plan(excluded_nodes, expected_subgraphs):
    graph = to_graph(GRAPH_2)
    execution_plan = ExecutionPlan(
        [node for node in graph.nodes if node not in excluded_nodes], "<test>"
    )
    new_graph = plan_graph(graph, execution_plan)

    subgraphs = [
        maybe_subgraph
        for maybe_subgraph in new_graph.nodes
        if isinstance(maybe_subgraph, nx.DiGraph)
    ]
    assert {frozenset(subgraph.nodes) for subgraph in subgraphs} == expected_subgraphs
