from dataclasses import dataclass
from os import name
from typing import Dict, List, Optional, List, Any, Callable
from pydantic import BaseModel
from google.cloud import bigquery, bigquery_storage
from dbt.parse import parse_profile


class FalGeneralException(Exception):
    pass


class DbtNodeDeps(BaseModel):
    nodes: List[str]


class DbtNodeConfig(BaseModel):
    materialized: Optional[str]


class Node(BaseModel):
    unique_id: str
    path: str
    resource_type: str
    description: str
    depends_on: Optional[DbtNodeDeps]
    config: DbtNodeConfig
    relation_name: Optional[str]


class DbtManifest(BaseModel):
    nodes: Dict[str, Node]
    sources: Dict[str, Node]
    metadata: Dict[str, Any]


class DbtModel(BaseModel):
    name: str
    meta: Any
    description: str
    columns: Any

    def model_key(self, project_name):
        return "model." + project_name + "." + self.name


class DbtProfileOutput(BaseModel):
    target: str


class DbtProfile(BaseModel):
    target: str
    outputs: List[DbtProfileOutput]


class DbtProfileFile(BaseModel):
    profiles: List[DbtProfile]


@dataclass
class DbtProject:
    name: str
    model_config_paths: List[str]
    models: List[DbtModel]
    manifest: DbtManifest
    keyword: str
    meta_filter_parser: Callable

    def filter_models(self) -> List[DbtModel]:
        return list(
            filter(lambda model: self.meta_filter_parser(model.meta), self.models)
        )

    def state_has_changed(self, other: DbtManifest) -> bool:
        return self.manifest != other

    def find_model_location(self, model: DbtModel) -> List[str]:
        model_node = self.manifest.nodes[model.model_key(self.name)]
        return model_node.relation_name

    def get_credentials(self, profile_name: str, credential_name: str):
        profile = parse_profile(None, self.name)
        pass

    def get_materilization_type(self, model: DbtModel) -> str:
        model_node = self.manifest.nodes[model.model_key(self.name)]
        config = model_node["config"]["materialized"]
        return config

    def get_data_frame(self, table_id: str):
        db_type = self.manifest.metadata["adapter_type"]
        if db_type == "bigquery":
            rows = bigquery.Client().list_rows(
                bigquery.TableReference.from_string(table_id)
            )
            client = bigquery_storage.BigQueryReadClient()
            return rows.to_dataframe(bqstorage_client=client)
        else:
            raise FalGeneralException(db_type + "is not supported in Fal yet.")
