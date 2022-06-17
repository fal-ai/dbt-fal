from collections import defaultdict

import networkx as nx
import pytest

from fal.node_graph import DbtModelNode, NodeGraph, NodeKind
from fal.planner.plan import plan_graph
from fal.planner.schedule import SUCCESS, schedule_graph
from fal.planner.tasks import DBTTask, FalModelTask
from tests.planner.data import GRAPH_1, GRAPHS
from tests.planner.utils import to_graph


class ModelDict(defaultdict):
    def get(self, key) -> None:
        return super().__getitem__(key)


def test_scheduler():
    graph = to_graph(GRAPH_1)
    new_graph = plan_graph(graph)
    node_graph = NodeGraph(graph, ModelDict(lambda: DbtModelNode("...", None)))
    node_queue = schedule_graph(new_graph, node_graph)

    # A -> B \
    #          -> E -> F
    # C -> D /

    node_A, node_C = node_queue.iter_available_nodes()
    assert node_A.task.model_ids == ["A"]
    assert node_C.task.model_ids == ["C"]

    # When both A and C are still running, the scheduler shouldn't
    # yield anything.
    assert len(list(node_queue.iter_available_nodes())) == 0

    # But when A is unblocked, it can successfully yield B
    node_queue.finish(node_A, SUCCESS)
    (node_B,) = node_queue.iter_available_nodes()
    assert node_B.task.model_ids == ["B"]

    # The rest of the graph is still blocked
    assert len(list(node_queue.iter_available_nodes())) == 0

    # When C is done, it should yield D
    node_queue.finish(node_C, SUCCESS)
    (node_D,) = node_queue.iter_available_nodes()
    assert node_D.task.model_ids == ["D"]

    # And when both B and D are done, it will yield E
    node_queue.finish(node_B, SUCCESS)
    node_queue.finish(node_D, SUCCESS)
    (node_E,) = node_queue.iter_available_nodes()
    assert node_E.task.model_ids == ["E"]

    # And finally when E is done, it will yield F
    node_queue.finish(node_E, SUCCESS)
    (node_F,) = node_queue.iter_available_nodes()
    assert node_F.task.model_ids == ["F"]


@pytest.mark.parametrize("graph_info", GRAPHS)
def test_scheduler_task_separation(graph_info):
    graph = graph_info["graph"]
    new_graph = plan_graph(graph)
    node_graph = NodeGraph(graph, ModelDict(lambda: DbtModelNode("...", None)))
    node_queue = schedule_graph(new_graph, node_graph)

    all_dbt_tasks, all_fal_tasks, all_post_hooks = set(), set(), set()
    for node in node_queue.nodes:
        if isinstance(node.task, FalModelTask):
            all_fal_tasks.update(node.task.model_ids)
        elif isinstance(node.task, DBTTask):
            all_dbt_tasks.update(node.task.model_ids)

        all_post_hooks.update(post_hook.hook_path for post_hook in node.post_hooks)

    assert all_dbt_tasks == set(
        nx.subgraph_view(
            graph,
            lambda node: graph.nodes[node]["kind"] is NodeKind.DBT_MODEL,
        )
    )
    assert all_fal_tasks == set(
        nx.subgraph_view(
            graph,
            lambda node: graph.nodes[node]["kind"] is NodeKind.FAL_MODEL,
        )
    )
    assert all_post_hooks == {
        post_hook
        for properties in graph.nodes.values()
        for post_hook in properties.get("post-hook", [])
    }


@pytest.mark.parametrize("graph_info", GRAPHS)
def test_scheduler_dependency_management(graph_info):
    graph = graph_info["graph"]
    new_graph = plan_graph(graph)
    node_graph = NodeGraph(graph, ModelDict(lambda: DbtModelNode("...", None)))
    node_queue = schedule_graph(new_graph, node_graph)

    while node_queue:
        for node in node_queue.iter_available_nodes():
            assert not node.dependencies
            node_queue.finish(node, SUCCESS)
