from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple, Dict

from fal.fal_script import FalScript
from faldbt.project import DbtModel, FalDbt
from pathlib import Path
import networkx as nx
import os as os
import copy
from functools import reduce


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
    pass


def _add_after_scripts(
    model: DbtModel,
    upstream_fal_node_unique_id: str,
    keyword: str,
    project_dir: str,
    graph: nx.DiGraph,
    nodeLookup: Dict[str, FalFlowNode],
) -> Tuple[nx.DiGraph, Dict[str, FalFlowNode]]:
    "Add dbt node to after scripts edges to the graph"
    after_scripts = model.get_script_paths(keyword, project_dir, False)
    after_fal_scripts = map(
        lambda script_path: FalScript(model, script_path), after_scripts
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
        graph.add_node(fal_script_node.unique_id)
        nodeLookup[fal_script_node.unique_id] = fal_script_node
        # model_fal_node depends on fal_script_node
        graph.add_edge(upstream_fal_node_unique_id, fal_script_node.unique_id)

    return graph, nodeLookup


def _add_before_scripts(
    model: DbtModel,
    downstream_fal_node_unique_id: str,
    keyword: str,
    project_dir: str,
    graph: nx.DiGraph,
    nodeLookup: Dict[str, FalFlowNode],
) -> Tuple[nx.DiGraph, Dict[str, FalFlowNode]]:
    "Add before scripts to dbt node edges to the graph"
    before_scripts = model.get_script_paths(keyword, project_dir, True)
    before_fal_scripts = map(
        lambda script_path: FalScript(model, script_path), before_scripts
    )
    before_fal_script_node = map(
        lambda fal_script: ScriptNode(
            _script_id_from_path(fal_script.path, model.name, "BEFORE"), fal_script
        ),
        before_fal_scripts,
    )

    for fal_script_node in before_fal_script_node:
        graph.add_node(fal_script_node.unique_id)
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
            graph.add_node(model_fal_node.unique_id)

            # Add dbt model dependencies
            for dbt_dependency_unique_id in model_fal_node.model.get_depends_on_nodes():
                graph.add_node(dbt_dependency_unique_id)
                graph.add_edge(dbt_dependency_unique_id, model_fal_node.unique_id)

            _add_after_scripts(
                model,
                model_fal_node.unique_id,
                fal_dbt.keyword,
                fal_dbt.project_dir,
                graph,
                node_lookup,
            )

            _add_before_scripts(
                model,
                model_fal_node.unique_id,
                fal_dbt.keyword,
                fal_dbt.project_dir,
                graph,
                node_lookup,
            )

        return cls(graph, node_lookup)

    def __init__(self, graph: nx.DiGraph, node_lookup: Dict[str, FalFlowNode]):
        self.graph = graph
        self.node_lookup = node_lookup

    def get_descendants(self, id: str) -> List[str]:
        return list(nx.descendants(self.graph, id))

    def get_predecessors(self, id: str) -> List[str]:
        return list(self.graph.predecessors(id))

    def get_node(self, id: str) -> FalFlowNode | None:
        return self.node_lookup.get(id)

    def _is_script_node(self, node_name: str) -> bool:
        return node_name.endswith(".py")

    def is_critical_node(self, node):
        successors = list(self.graph.successors(node))
        if node in successors:
            successors.remove(node)
        has_script_pred = lambda node_name: node_name.endswith(".py")
        has_model_pred = lambda node_name: node_name.split(".")[0] == "model"
        if any(has_script_pred(i) for i in successors) and any(
            has_model_pred(i) for i in successors
        ):
            return True

    def sort_nodes(self):
        return nx.topological_sort(self.graph)

    def generate_safe_subgraphs(self):
        nodes = list(self.sort_nodes())
        buckets = []
        local_bucket = []
        seen_nodes = []
        # critical_node = next((node for node in nodes if self.is_critical_node(node)), None)
        for index, node in enumerate(nodes):
            if node not in seen_nodes:
                local_bucket.append(node)
                seen_nodes.append(node)
                if self.is_critical_node(node):
                    script_successors = list(
                        filter(
                            lambda successor: successor.endswith(".py"),
                            self.graph.successors(node),
                        )
                    )
                    seen_nodes.extend(script_successors)
                    local_bucket.extend(script_successors)
                    buckets.append(local_bucket)
                    local_bucket = []
        buckets.append(local_bucket)
        sub_graphs = []
        for bucket in buckets:
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

        # do topo sort and then get the critical nodes one by one outside

        #     def nodes_with_after_scripts(self):
        # nodes = []
        # for node in list(self.graph.nodes()):
        #     successors = list(self.graph.successors(node))
        #     if node in successors:
        #         successors.remove(node)
        #     has_script_pred = lambda node_name: node_name.endswith(".py")
        #     has_model_pred = lambda node_name: node_name.split(".")[0] == "model"
        #     if any(has_script_pred(i) for i in successors) and any(
        #         has_model_pred(i) for i in successors
        #     ):
        #         nodes.append(node)
        # return nodes

        # critical_nodes = self.nodes_with_after_scripts()
        # descendants_of_critical_nodes = set(
        #     map(
        #         lambda node: list(self.get_descendants(node)),
        #         filter(
        #             lambda node: not node.split(".")[0] == "model",
        #             reduce(
        #                 lambda list, other: list + other,
        #                 list(
        #                     map(
        #                         lambda node: list(self.graph.successors(node)),
        #                         critical_nodes,
        #                     )
        #                 ),
        #             ),
        #         ),
        #     )
        # )
        # for critical_node in critical_nodes:
        #     if critical_node in descendants_of_critical_nodes:
        #         descendants_of_critical_nodes.remove(critical_node)
        # graph_copy = copy.deepcopy(self.graph)
        # for successor_node in descendants_of_critical_nodes:
        #     graph_copy.remove_node(successor_node)
        # return graph_copy
