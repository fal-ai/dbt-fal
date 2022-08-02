from re import L
from logbook import StreamHandler
import json
import sys
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import Any, Dict


def reconfigure_handlers():
    from logbook import INFO, StreamHandler
    from dbt.logger import GLOBAL_LOGGER as logger

    # Reconfigure DBT's log handlers to output to
    # stdout so we can stream it back to the parent
    # process.
    logger.disabled = False
    logger.handlers.append(StreamHandler(sys.stdout))
    logger.level = INFO

def run_fal_hook(
    path: str,
    bound_model_name: str,
    fal_dbt_config: Dict[str, Any],
    arguments: Dict[str, Any],
    run_index: int,
) -> None:
    from fal.node_graph import DbtModelNode, NodeGraph
    from fal.planner.tasks import FalLocalHookTask
    from faldbt.project import FalDbt

    fal_dbt = FalDbt(**fal_dbt_config)
    node_graph = NodeGraph.from_fal_dbt(fal_dbt)
    flow_node = node_graph.get_node(bound_model_name)
    assert isinstance(flow_node, DbtModelNode)

    task = FalLocalHookTask(path, flow_node.model, arguments)
    task._run_index = run_index

    dummy_namespace = Namespace()
    return task.execute(dummy_namespace, fal_dbt)


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("data", type=str)

    options = parser.parse_args()
    exit_code = run_fal_hook(**json.loads(options.data))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
