from dataclasses import dataclass, field
from os import name
from typing import Dict, List, Optional, List, Any, TypeVar
from dbt.contracts.graph.parsed import ParsedModelNode
from dbt.node_types import NodeType
from pydantic import BaseModel
from pathlib import Path
from dbt.contracts.graph.manifest import Manifest
import faldbt.lib as lib
import pandas as pd


class FalGeneralException(Exception):
    pass


@dataclass
class DbtModel:
    node: ParsedModelNode
    name: str = field(init=False)
    meta: Dict[str, Any] = field(init=False)

    def __post_init__(self):
        self.name = self.node.name
        self.meta = self.node.config.meta

    def model_key(self, project_name):
        return "model." + project_name + "." + self.name


@dataclass
class DbtManifest:
    nativeManifest: Manifest

    def get_models(self) -> List[DbtModel]:
        return list(
            filter(
                lambda model: model.node.resource_type == NodeType.Model,
                map(
                    lambda node: DbtModel(node=node), self.nativeManifest.nodes.values()
                ),
            )
        )

    def get_models_with_keyword(self, keyword) -> List[DbtModel]:
        return list(filter(lambda model: keyword in model.meta, self.get_models()))


class DbtRunResult(BaseModel):
    status: str
    timing: List[Any]
    thread_id: str
    execution_time: int
    adapter_response: Dict[str, str]
    message: str
    failures: Any
    unique_id: str


class DbtRunResultFile(BaseModel):
    metadata: Any
    results: List[DbtRunResult]


T = TypeVar("T", bound="DbtProject")


@dataclass
class DbtProject:
    name: str
    model_config_paths: List[str]
    models: List[DbtModel]
    manifest: DbtManifest
    keyword: str
    scripts: List[Path]
    results: DbtRunResultFile

    def state_has_changed(self, other: DbtManifest) -> bool:
        return self.manifest != other

    def find_model_location(self, model: DbtModel) -> List[str]:
        model_node = self.manifest.nodes[model.model_key(self.name)]
        return model_node.relation_name.replace("`", "")

    def changed_model_names(self) -> List[str]:
        return list(
            map(lambda result: result.unique_id.split(".")[-1], self.results.results)
        )

    def get_filtered_models(self, all):
        filtered_models: List[ParsedModelNode] = []
        for node in self.manifest.get_models_with_keyword(self.keyword):
            if all:
                filtered_models.append(node)
            elif node.name in self.changed_model_names():
                filtered_models.append(node)
            else:
                continue
        return filtered_models
