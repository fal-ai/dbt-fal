import networkx as nx
import pytest

from fal.node_graph import NodeKind
from fal.planner.tasks import FAILURE, SUCCESS, DBTTask, FalModelTask
from fal.planner.tasks import DBTTask, FalModelTask, GroupStatus
from tests.planner.data import GRAPH_1, GRAPHS
from tests.planner.utils import to_scheduler


def test_scheduler():
    scheduler = to_scheduler(GRAPH_1)

    # A -> B \
    #          -> E -> F
    # C -> D /

    group_A, group_C = scheduler.iter_available_groups()
    assert group_A.task.model_ids == ["A"]
    assert group_C.task.model_ids == ["C"]

    # When both A and C are still running, the scheduler shouldn't
    # yield anything.
    assert len(list(scheduler.iter_available_groups())) == 0
    assert_running(scheduler, "A", "C")

    # But when A is unblocked, it can successfully yield B
    scheduler.finish(group_A, SUCCESS)
    (group_B,) = scheduler.iter_available_groups()
    assert group_B.task.model_ids == ["B"]
    assert_running(scheduler, "B", "C")

    # The rest of the graph is still blocked
    assert len(list(scheduler.iter_available_groups())) == 0

    # When C is done, it should yield D
    scheduler.finish(group_C, SUCCESS)
    (group_D,) = scheduler.iter_available_groups()
    assert group_D.task.model_ids == ["D"]
    assert_running(scheduler, "B", "D")

    # And when both B and D are done, it will yield E
    scheduler.finish(group_B, SUCCESS)
    scheduler.finish(group_D, SUCCESS)
    (group_E,) = scheduler.iter_available_groups()
    assert group_E.task.model_ids == ["E"]
    assert_running(scheduler, "E")

    # And finally when E is done, it will yield F
    scheduler.finish(group_E, SUCCESS)
    (group_F,) = scheduler.iter_available_groups()
    assert group_F.task.model_ids == ["F"]
    assert_running(scheduler, "F")


def assert_running(scheduler, *tasks):
    assert {
        skipped_model
        for group in scheduler.filter_groups(GroupStatus.RUNNING)
        for skipped_model in group.task.model_ids
    } == set(tasks)


def assert_skipped(scheduler, *tasks):
    assert {
        skipped_model
        for group in scheduler.filter_groups(GroupStatus.SKIPPED)
        for skipped_model in group.task.model_ids
    } == set(tasks)


def assert_failed(scheduler, *tasks):
    assert {
        failed_model
        for group in scheduler.filter_groups(GroupStatus.FAILURE)
        for failed_model in group.task.model_ids
    } == set(tasks)


def test_scheduler_error_handling():
    scheduler = to_scheduler(GRAPH_1)

    # A -> B \
    #          -> E -> F
    # C -> D /

    # Run A and C as usual
    group_A, group_C = scheduler.iter_available_groups()
    assert group_A.task.model_ids == ["A"]
    assert group_C.task.model_ids == ["C"]
    scheduler.finish(group_A, SUCCESS)

    # But once A is completed, take B and make it fail.
    (group_B,) = scheduler.iter_available_groups()
    assert group_B.task.model_ids == ["B"]
    scheduler.finish(group_B, FAILURE)

    # B's failure shouldn't affect C or D, since they are
    # completely independant.
    scheduler.finish(group_C, SUCCESS)
    (group_D,) = scheduler.iter_available_groups()
    assert group_D.task.model_ids == ["D"]

    # When D is done, we won't have any more tasks to continue
    # since E and F rrequires B which just failed.
    scheduler.finish(group_D, SUCCESS)
    assert len(list(scheduler.iter_available_groups())) == 0

    # Ensure that only B has failed.
    assert_failed(scheduler, "B")
    assert_skipped(scheduler, "E", "F")


@pytest.mark.parametrize("graph_info", GRAPHS)
def test_scheduler_task_separation(graph_info):
    graph = graph_info["graph"]
    scheduler = to_scheduler(graph)

    all_dbt_tasks, all_fal_tasks, all_post_hooks = set(), set(), set()
    for group in scheduler.pending_groups:
        if isinstance(group.task, FalModelTask):
            all_fal_tasks.update(group.task.model_ids)
        elif isinstance(group.task, DBTTask):
            all_dbt_tasks.update(group.task.model_ids)

        all_post_hooks.update(post_hook.hook_path for post_hook in group.post_hooks)

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
        for post_hook in properties.get("post_hook", [])
    }


@pytest.mark.parametrize("graph_info", GRAPHS)
def test_scheduler_dependency_management(graph_info):
    scheduler = to_scheduler(graph_info["graph"])

    while scheduler:
        for group in scheduler.iter_available_groups():
            assert not group.dependencies
            scheduler.finish(group, SUCCESS)
