from dataclasses import dataclass, field
from typing import Dict, List, List, Any, TypeVar, Sequence
from dbt.contracts.graph.parsed import ParsedModelNode
from dbt.node_types import NodeType
from pathlib import Path
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import RunResultsArtifact, RunResultOutput


class FalGeneralException(Exception):
    pass


@dataclass
class DbtModel:
    node: ParsedModelNode
    name: str = field(init=False)
    meta: Dict[str, Any] = field(init=False)
    status: str = field(init=False)
    columns: Dict[str, Any] = field(init=False)

    def __post_init__(self):
        self.name = self.node.name
        self.meta = self.node.config.meta
        self.columns = self.node.columns

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


@dataclass
class DbtRunResult:
    nativeRunResult: RunResultsArtifact
    results: Sequence[RunResultOutput] = field(init=False)

    def __post_init__(self):
        self.results = self.nativeRunResult.results


T = TypeVar("T", bound="DbtProject")


@dataclass
class DbtProject:
    name: str
    model_config_paths: List[str]
    models: List[DbtModel]
    manifest: DbtManifest
    keyword: str
    scripts: List[Path]
    run_result: DbtRunResult

    def state_has_changed(self, other: DbtManifest) -> bool:
        return self.manifest != other

    def find_model_location(self, model: DbtModel) -> List[str]:
        model_node = self.manifest.nodes[model.model_key(self.name)]
        return model_node.relation_name.replace("`", "")

    def changed_model_names(self) -> List[str]:
        return list(
            map(
                lambda result: result["unique_id"].split(".")[-1],
                self.run_result.results,
            )
        )

    def get_models_with_keyword(self, keyword) -> List[DbtModel]:
        return list(filter(lambda model: keyword in model.meta, self.models))

    def get_filtered_models(self, all):
        filtered_models: List[DbtModel] = []
        for node in self.get_models_with_keyword(self.keyword):
            if all:
                filtered_models.append(node)
            elif node.name in self.changed_model_names():
                filtered_models.append(node)
            else:
                continue
        return filtered_models
