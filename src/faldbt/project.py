import os
from dataclasses import dataclass, field
from typing import Dict, List, List, Any, Optional, TypeVar, Sequence, Union
from pathlib import Path

from dbt.node_types import NodeType
from dbt.config import RuntimeConfig
from dbt.contracts.graph.parsed import ParsedModelNode, ParsedSourceDefinition
from dbt.contracts.graph.manifest import Manifest, MaybeNonSource, MaybeParsedSource
from dbt.contracts.results import RunResultsArtifact, RunResultOutput
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.adapters.bigquery.connections import BigQueryConnectionManager
import dbt.tracking

from . import parse
from . import lib

import firebase_admin
from firebase_admin import firestore
from google.cloud.firestore import Client as FirestoreClient
import uuid

from decimal import Decimal
import pandas as pd


class FalGeneralException(Exception):
    pass


def normalize_directories(base: str, dirs: List[str]) -> List[Path]:
    return list(
        map(
            lambda dir: Path(os.path.realpath(os.path.join(base, dir))),
            dirs,
        )
    )


@dataclass
class DbtModel:
    node: ParsedModelNode
    name: str = field(init=False)
    meta: Dict[str, Any] = field(init=False)
    status: str = field(init=False)
    columns: Dict[str, Any] = field(init=False)

    def __post_init__(self):
        node = self.node
        self.name = node.name
        if hasattr(node.config, "meta"):
            self.meta = node.config.meta
        elif hasattr(node, "meta"):
            self.meta = node.meta
        self.columns = node.columns
        self.unique_id = node.unique_id

    def __hash__(self) -> int:
        return self.unique_id.__hash__()

    def get_script_paths(self, keyword, project_dir) -> List[Path]:
        return normalize_directories(project_dir, self.get_scripts(keyword))

    def get_scripts(self, keyword) -> List[Path]:
        # sometimes `scripts` can *be* there and still be None
        return self.meta[keyword].get("scripts") or []

    def set_status(self, status: str):
        self.status = status


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

    def get_sources(self) -> List[ParsedSourceDefinition]:
        return list(self.nativeManifest.sources.values())


@dataclass
class DbtRunResult:
    nativeRunResult: RunResultsArtifact
    results: Sequence[RunResultOutput] = field(init=False)

    def __post_init__(self):
        self.results = self.nativeRunResult.results


@dataclass(init=False)
class FalDbt:
    project_dir: str
    profiles_dir: str
    keyword: str

    _config: RuntimeConfig
    _manifest: DbtManifest
    _run_results: DbtRunResult

    _model_status_map: Dict[str, str]

    _global_script_paths = List[str]

    _firestore_client: Union[FirestoreClient, None]

    def __init__(self, project_dir: str,
                 profiles_dir: str,
                 keyword: str = 'fal'):
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.keyword = keyword
        self._config = parse.get_dbt_config(project_dir, profiles_dir)
        lib.register_adapters(self._config)

        # Necessary for parse_to_manifest to not fail
        dbt.tracking.initialize_tracking(profiles_dir)

        self._manifest = DbtManifest(parse.get_dbt_manifest(self._config))

        self._run_results = DbtRunResult(
            parse.get_dbt_results(self.project_dir, self._config)
        )

        normalized_source_paths = normalize_directories(
            project_dir, self._config.source_paths
        )

        self._global_script_paths = parse.get_global_script_configs(
            normalized_source_paths
        )

        self._model_status_map = dict(
            map(
                lambda res: [res.unique_id, res.status],
                self._run_results.results,
            )
        )

        self._setup_firestore()

    def get_model_status(self, unique_id: str):
        # Default to `skipped` status if not found, it means it did not run
        return self._model_status_map.get(unique_id, "skipped")

    def list_sources(self):
        """
        List tables available for `source` usage, formatting like `[[source_name, table_name], ...]`
        """
        res = []
        for source in self._manifest.get_sources():
            res.append([source.source_name, source.name])

        return res

    def list_models_ids(self) -> Dict[str, str]:
        """
        List model ids available for `ref` usage, formatting like `[ref_name, ...]`
        """
        res = {}
        for model in self._manifest.get_models():
            res[model.unique_id] = self.get_model_status(model.unique_id)

        return res

    def list_models(self) -> List[DbtModel]:
        """
        List models
        """
        models = []
        for model in self._manifest.get_models():
            model.set_status(self.get_model_status(model.unique_id))
            models.append(model)
        return models

    def ref(
        self, target_model_name: str, target_package_name: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Download a dbt model as a pandas.DataFrame automagically.
        """

        target_model: MaybeNonSource = self._manifest.nativeManifest.resolve_ref(
            target_model_name, target_package_name, self.project_dir, self.project_dir
        )

        if target_model is None:
            raise Exception(
                f"Could not find model {target_model_name}.{target_package_name or ''}"
            )

        result = lib.fetch_target(
            self._manifest.nativeManifest,
            self.project_dir,
            self.profiles_dir,
            target_model,
        )
        return pd.DataFrame.from_records(
            result.table.rows, columns=result.table.column_names
        )

    def source(self, target_source_name: str, target_table_name: str) -> pd.DataFrame:
        """
        Download a dbt source as a pandas.DataFrame automagically.
        """

        target_source: MaybeNonSource = self._manifest.nativeManifest.resolve_source(
            target_source_name, target_table_name, self.project_dir, self.project_dir
        )

        if target_source is None:
            raise Exception(
                f"Could not find source '{target_source_name}'.'{target_table_name}'"
            )

        result = lib.fetch_target(
            self._manifest.nativeManifest,
            self.project_dir,
            self.profiles_dir,
            target_source,
        )
        return pd.DataFrame.from_records(
            result.table.rows, columns=result.table.column_names
        )

    def write_to_source(
        self, data: pd.DataFrame, target_source_name: str, target_table_name: str
    ):
        """
        Write a pandas.DataFrame to a dbt source automagically.
        """

        target_source: MaybeParsedSource = self._manifest.nativeManifest.resolve_source(
            target_source_name, target_table_name, self.project_dir, self.project_dir
        )

        if target_source is None:
            raise Exception(
                f"Could not find source '{target_source_name}'.'{target_table_name}'"
            )

        lib.write_target(
            data,
            self._manifest.nativeManifest,
            self.project_dir,
            self.profiles_dir,
            target_source,
        )

    def write_to_firestore(self, df: pd.DataFrame, collection: str, key_column: str):
        """
        Write a pandas.DataFrame to a GCP Firestore collection. You must specify the column to use as key.
        """

        if self._firestore_client is None:
            raise FalGeneralException(
                "GCP credentials not setup correctly. Check warnings during initialization."
            )

        df_arr = df.to_dict("records")
        for item in df_arr:
            key = item[key_column]
            data = _firestore_dict_to_document(data=item, key_column=key_column)
            self._firestore_client.collection(collection).document(str(key)).set(data)

    def _setup_firestore(self):
        app_name = f"fal-{uuid.uuid4()}"
        profile_cred = self._config.credentials

        # Setting projectId from the profiles.yml
        options = {"projectId": profile_cred.database}
        app = None

        try:
            # Try with the profiles.yml credentials
            if profile_cred.type != "bigquery":
                raise Exception("To be caught")

            # HACK: using internal method of Bigquery adapter to mock a Firebase credential
            cred = firebase_admin.credentials.ApplicationDefault()
            cred._g_credential = BigQueryConnectionManager.get_bigquery_credentials(
                profile_cred
            )

            app = firebase_admin.initialize_app(cred, options, name=app_name)

            self._firestore_client = firestore.client(app=app)

        except Exception:
            logger.warn(
                "Could not find acceptable GCP credentials in profiles.yml, trying default GCP Application"
            )

            if app:
                firebase_admin.delete_app(app)

            try:
                # Use the application default credentials
                cred = firebase_admin.credentials.ApplicationDefault()

                app = firebase_admin.initialize_app(cred, options, name=app_name)

                self._firestore_client = firestore.client(app=app)
            except Exception:
                logger.warn(
                    "Could not find acceptable Default GCP Application credentials"
                )


def _firestore_dict_to_document(data: Dict, key_column: str):
    output = {}
    for (k, v) in data.items():
        if k == key_column:
            continue
        # TODO: Add more type conversions here
        if isinstance(v, Decimal):
            output[k] = str(v)
        else:
            output[k] = v
    return output


T = TypeVar("T", bound="FalProject")


@dataclass(init=False)
class FalProject:
    keyword: str
    scripts: List[Path]

    _faldbt: FalDbt

    def __init__(
        self,
        faldbt: FalDbt,
    ):
        self._faldbt = faldbt
        self.keyword = faldbt.keyword
        self.scripts = parse.get_scripts_list(faldbt.project_dir)

    def get_model_status(self, model: DbtModel):
        return self._faldbt.get_model_status(model.unique_id)

    def _result_model_ids(self) -> List[str]:
        return list(map(lambda r: r.unique_id, self._faldbt._run_results.results))

    def _get_models_with_keyword(self, keyword) -> List[DbtModel]:
        return list(
            filter(lambda model: keyword in model.meta, self._faldbt.list_models())
        )

    def get_filtered_models(self, all) -> List[DbtModel]:
        models_ids = self._result_model_ids()
        filtered_models: List[DbtModel] = []

        for node in self._get_models_with_keyword(self.keyword):
            if all:
                filtered_models.append(node)
            elif node.unique_id in models_ids:
                filtered_models.append(node)

        return filtered_models
