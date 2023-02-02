from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple, Dict

from fal.fal_script import FalScript
from faldbt.project import DbtModel, FalDbt
from pathlib import Path
import networkx as nx
import os as os
from functools import reduce
from enum import Enum


class NodeKind(str, Enum):
    DBT_MODEL = "dbt model"
    FAL_MODEL = "fal model"
    FAL_SCRIPT = "fal script"


@dataclass
class FalFlowNode:
    "Represents a Node that can be invoked by fal flow command"
    unique_id: str


@dataclass
class ScriptNode(FalFlowNode):
    "Represents a python script node"
    script: FalScript


@dataclass
class DbtModelNode(FalFlowNode):
    "Represents a dbt node"
    model: DbtModel


def _add_after_scripts(
    model: DbtModel,
    upstream_fal_node_unique_id: str,
    faldbt: FalDbt,
    graph: nx.DiGraph,
    nodeLookup: Dict[str, FalFlowNode],
) -> Tuple[nx.DiGraph, Dict[str, FalFlowNode]]:
    "Add dbt node to after scripts edges to the graph"
    after_scripts = model.get_scripts(before=False)
    after_fal_scripts = map(
        lambda script_path: FalScript(faldbt, model, script_path), after_scripts
    )
    after_fal_script_nodes = list(
        map(
            lambda fal_script: ScriptNode(
                _script_id_from_path(fal_script.path, model.name, "AFTER"), fal_script
            ),
            after_fal_scripts,
        )
    )
    for fal_script_node in after_fal_script_nodes:
        graph.add_node(fal_script_node.unique_id, kind=NodeKind.FAL_SCRIPT)
        nodeLookup[fal_script_node.unique_id] = fal_script_node
        # model_fal_node depends on fal_script_node
        graph.add_edge(upstream_fal_node_unique_id, fal_script_node.unique_id)

    return graph, nodeLookup


def _add_before_scripts(
    model: DbtModel,
    downstream_fal_node_unique_id: str,
    faldbt: FalDbt,
    graph: nx.DiGraph,
    nodeLookup: Dict[str, FalFlowNode],
) -> Tuple[nx.DiGraph, Dict[str, FalFlowNode]]:
    "Add before scripts to dbt node edges to the graph"
    before_scripts = model.get_scripts(before=True)
    before_fal_scripts = map(
        lambda script_path: FalScript(faldbt, model, script_path), before_scripts
    )
    before_fal_script_node = map(
        lambda fal_script: ScriptNode(
            _script_id_from_path(fal_script.path, model.name, "BEFORE"), fal_script
        ),
        before_fal_scripts,
    )

    for fal_script_node in before_fal_script_node:
        graph.add_node(fal_script_node.unique_id, kind=NodeKind.FAL_SCRIPT)
        nodeLookup[fal_script_node.unique_id] = fal_script_node
        # fal_script_node depends on model_fal_node
        graph.add_edge(fal_script_node.unique_id, downstream_fal_node_unique_id)

    return graph, nodeLookup


def _script_id_from_path(scriptPath: Path, modelName: str, order: str):
    script_file_name = os.path.basename(scriptPath)
    return f"script.{modelName}.{order}.{script_file_name}"


@dataclass
class NodeGraph:
    "Wrapper around networkx graph"

    @classmethod
    def from_fal_dbt(cls, fal_dbt: FalDbt):
        graph = nx.DiGraph()
        node_lookup: Dict[str, FalFlowNode] = {}
        for model in fal_dbt.list_models():
            model_fal_node = DbtModelNode(model.unique_id, model)
            node_lookup[model_fal_node.unique_id] = model_fal_node
            graph.add_node(
                model_fal_node.unique_id,
                kind=NodeKind.FAL_MODEL if model.python_model else NodeKind.DBT_MODEL,
                post_hook=model.get_post_hook_paths(),
                pre_hook=model.get_pre_hook_paths(),
                environment=model.environment_name,
            )

            # Add dbt model dependencies
            for dbt_dependency_unique_id in model_fal_node.model.get_depends_on_nodes():
                if dbt_dependency_unique_id not in node_lookup:
                    graph.add_node(dbt_dependency_unique_id, kind=NodeKind.DBT_MODEL)
                graph.add_edge(dbt_dependency_unique_id, model_fal_node.unique_id)

            _add_after_scripts(
                model,
                model_fal_node.unique_id,
                fal_dbt,
                graph,
                node_lookup,
            )

            _add_before_scripts(
                model,
                model_fal_node.unique_id,
                fal_dbt,
                graph,
                node_lookup,
            )

        return cls(graph, node_lookup)

    def __init__(self, graph: nx.DiGraph, node_lookup: Dict[str, FalFlowNode]):
        self.graph = graph
        self.node_lookup = node_lookup

    def get_successors(self, id: str, levels: int) -> List[str]:
        assert levels >= 0
        if levels == 0:
            return []
        else:
            current: List[str] = list(self.graph.successors(id))
            return reduce(
                lambda acc, id: acc + self.get_successors(id, levels - 1),
                current,
                current,
            )

    def get_descendants(self, id: str) -> List[str]:
        return list(nx.descendants(self.graph, id))

    def get_predecessors(self, id: str, levels: int) -> List[str]:
        assert levels >= 0
        if levels == 0:
            return []
        else:
            current: List[str] = list(self.graph.predecessors(id))
            return reduce(
                lambda acc, id: acc + self.get_predecessors(id, levels - 1),
                current,
                current,
            )

    def get_ancestors(self, id: str) -> List[str]:
        return list(nx.ancestors(self.graph, id))

    def get_node(self, id: str) -> FalFlowNode | None:
        return self.node_lookup.get(id)

    def _is_script_node(self, node_name: str) -> bool:
        return _is_script(node_name)

    def _is_critical_node(self, node):
        successors = list(self.graph.successors(node))
        if node in successors:
            successors.remove(node)

        def is_python_model(id: str):
            inode = self.get_node(id)
            if isinstance(inode, DbtModelNode):
                return inode.model.python_model
            return False

        def has_post_hooks(node_id: str):
            inode = self.get_node(node_id)
            if isinstance(inode, DbtModelNode):
                return bool(inode.model.get_post_hook_paths())
            return False

        is_model_pred = lambda node_name: node_name.split(".")[0] == "model"
        # fmt:off
        return (
            (any(_is_script(i) for i in successors) and
            any(is_model_pred(i) for i in successors)) or
            is_python_model(node) or
            has_post_hooks(node)
        )
        # fmt:on

    def sort_nodes(self):
        return nx.topological_sort(self.graph)

    def _group_nodes(self) -> List[List[str]]:
        nodes = list(self.sort_nodes())
        buckets = []
        local_bucket = []
        seen_nodes = []
        for node in nodes:
            if node not in seen_nodes:
                local_bucket.append(node)
                seen_nodes.append(node)
                if self._is_critical_node(node):
                    script_successors = list(
                        filter(
                            _is_script,
                            self.graph.successors(node),
                        )
                    )
                    seen_nodes.extend(script_successors)
                    local_bucket.extend(script_successors)
                    buckets.append(local_bucket)
                    local_bucket = []
        buckets.append(local_bucket)
        return buckets

    def generate_sub_graphs(self) -> List[NodeGraph]:
        "Generates subgraphs that are seperated by `critical nodes`"
        sub_graphs = []
        for bucket in self._group_nodes():
            sub_graph = self.graph.subgraph(bucket)
            sub_graph_nodes = list(sub_graph.nodes())
            local_lookup = dict(
                filter(
                    lambda node: node[0] in sub_graph_nodes, self.node_lookup.items()
                )
            )
            node_graph = NodeGraph(sub_graph, local_lookup)
            sub_graphs.append(node_graph)
        return sub_graphs


def _is_script(name: str) -> bool:
    return name.endswith(".py") or name.endswith(".ipynb")
