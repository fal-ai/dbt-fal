"""Fal Dagster integration."""
from copy import deepcopy
from typing import List, cast
import enum
from dagster import resource, op, graph, DynamicOut, DynamicOutput, get_dagster_logger
from argparse import Namespace
from fal.node_graph import NodeGraph, DbtModelNode
from faldbt.project import FalDbt
from fal.cli.selectors import ExecutionPlan
from fal.cli.args import parse_args
from fal.fal_script import FalScript
from fal.cli.fal_runner import create_fal_dbt
from fal.cli.flow_runner import _id_to_fal_scripts, _unique_ids_to_model_names
from fal.run_scripts import run_scripts, raise_for_run_results_failures
from fal.cli.dbt_runner import dbt_run, raise_for_dbt_run_errors
from fal.cli.model_generator import generate_python_dbt_models


class OpResultStatus(enum.Enum):
    success = "Success"
    failure = "Fail"


class FalResource:
    """Resource that allows you to access properties of a dbt-fal project."""

    project_dir: str
    parsed: Namespace
    fal_dbt: FalDbt
    node_graph: NodeGraph
    sub_graphs: List[NodeGraph]
    execution_plan: ExecutionPlan

    def __init__(self):
        """Initialize fal resource."""
        self.project_dir = "/opt/dbt_project"
        generated_models = generate_python_dbt_models(self.project_dir)
        command = " ".join(
            (
                "fal flow run",
                f"--profiles-dir {self.project_dir} --project-dir {self.project_dir}",
                "--experimental-models",
            )
        )
        self.parsed = parse_args(command.split()[1:])
        self.fal_dbt = create_fal_dbt(self.parsed, generated_models)
        self.node_graph = NodeGraph.from_fal_dbt(self.fal_dbt)
        self.execution_plan = ExecutionPlan.create_plan_from_graph(
            self.parsed, self.node_graph, self.fal_dbt
        )
        self._nodes = list(self.node_graph.graph.nodes())
        self.sub_graphs = self.node_graph.generate_sub_graphs()

    def get_dbt_nodes(self) -> List[DbtModelNode]:
        """Get dbt run nodes."""
        return list(
            filter(lambda node: node in self._nodes, self.execution_plan.dbt_models)
        )

    def get_script_from_id(self, script_id) -> FalScript:
        """Get a fal script from an id."""
        return _id_to_fal_scripts(self.node_graph, self.fal_dbt, [script_id])

    def get_dbt_model_names(self):
        """Get list of dbt model names."""
        return _unique_ids_to_model_names(self.get_dbt_nodes())

    def get_current_subgraph(self, index):
        """Get subgraph at index."""
        return self.sub_graphs[index]


@resource
def fal_resource() -> FalResource:
    """Initialize a dbt fal project."""
    return FalResource()


@op(out=DynamicOut(), required_resource_keys={"fal"})
def plan_before_runs(context, graph_index):
    """Prepare to run all before scripts in parallel."""
    node_graph = context.resources.fal.get_current_subgraph(graph_index)
    script_ids = [
        i
        for i in context.resources.fal.execution_plan.before_scripts
        if node_graph.get_node(i) is not None
    ]

    for index, script in enumerate(script_ids):
        yield DynamicOutput(script, str(index))


@op(out=DynamicOut(), required_resource_keys={"fal"})
def plan_after_runs(context, python_model_results, graph_index):
    """Prepare to run all after scripts in parallel."""
    node_graph = context.resources.fal.get_current_subgraph(graph_index)
    script_ids = [
        i
        for i in context.resources.fal.execution_plan.after_scripts
        if node_graph.get_node(i) is not None
    ]
    for index, script in enumerate(script_ids):
        yield DynamicOutput(script, str(index))


@op(required_resource_keys={"fal"})
def run_fal_script(context, script):
    """Run fal scripts individually."""
    logger = get_dagster_logger()
    logger.info(f"Running fal script: {script}")
    scripts = context.resources.fal.get_script_from_id(script)
    results = run_scripts(scripts, context.resources.fal.fal_dbt)
    raise_for_run_results_failures(scripts, results)
    return OpResultStatus.success


@op(required_resource_keys={"fal"})
def run_dbt(context, fal_run_results, graph_index):
    """Run dbt."""
    node_graph = context.resources.fal.get_current_subgraph(graph_index)
    parsed = deepcopy(context.resources.fal.parsed)
    logger = get_dagster_logger()
    logger.info("Running dbt node")
    if graph_index > 0:
        parsed.select = None
    node_list = list(node_graph.graph.nodes())
    dbt_nodes = list(
        filter(
            lambda node: node in node_list,
            context.resources.fal.execution_plan.dbt_models,
        )
    )
    dbt_model_names = _unique_ids_to_model_names(dbt_nodes)

    results = dbt_run(
        parsed,
        dbt_model_names,
        context.resources.fal.fal_dbt.target_path,
        0,
    )
    raise_for_dbt_run_errors(results)

    return OpResultStatus.success


@op(out=DynamicOut(), required_resource_keys={"fal"})
def plan_python_nodes(context, dbt_run_results, graph_index):
    """Find and yield fal python nodes."""
    node_graph = context.resources.fal.get_current_subgraph(graph_index)
    node_list = list(node_graph.graph.nodes())
    dbt_nodes = list(
        filter(
            lambda node: node in node_list,
            context.resources.fal.execution_plan.dbt_models,
        )
    )
    fal_nodes = []
    for n in dbt_nodes:
        mnode = cast(DbtModelNode, node_graph.get_node(n))
        if mnode is not None and mnode.model.python_model is not None:
            fal_nodes.append(n)
    if len(fal_nodes) != 0:
        for index, node in enumerate(fal_nodes):
            yield DynamicOutput(node, str(index))


@graph()
def run_sub_graph(index):
    """Run part of the fal graph."""
    before_plan = plan_before_runs(index)
    before_results = before_plan.map(run_fal_script)
    before_collected = before_results.collect()
    dbt_results = run_dbt(before_collected, index)
    python_model_plan = plan_python_nodes(dbt_results, index)
    python_model_results = python_model_plan.map(run_fal_script)
    python_model_collected = python_model_results.collect()
    after_plan = plan_after_runs(python_model_collected, index)
    after_results = after_plan.map(run_fal_script)
    collected_results = after_results.collect()

    # Sub graph has to return a result of an op
    return increment_index(index, collected_results)


@op()
def get_zero():
    """Get int 0."""
    return 0


@op
def increment_index(index, collected_results):
    """Increment index number by 1."""
    return index + 1


@graph
def flow_run():
    """Run the entire fal flow dag."""
    # Index needs to be passed to graph children and therefore has to come from
    # an op
    index = get_zero()

    # We need this in order to know how many sub_graphs there are and we cannot
    # and we have to know this before the dag is contructed and before any ops
    # are ran.
    fal_res = FalResource()

    for i in range(len(fal_res.sub_graphs)):
        index = run_sub_graph(index)
