from enum import Enum
import re
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple, Sequence
from pathlib import Path

from dbt.node_types import NodeType
from dbt.config import RuntimeConfig
from dbt.contracts.graph.parsed import ParsedModelNode, ParsedSourceDefinition
from dbt.contracts.graph.compiled import ManifestNode
from dbt.contracts.graph.manifest import (
    Manifest,
    MaybeNonSource,
    MaybeParsedSource,
    Disabled,
)
from dbt.contracts.results import RunResultsArtifact, RunResultOutput, NodeStatus
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.task.compile import CompileTask
import dbt.tracking

from . import parse
from . import lib
from .el_client import FalElClient

from fal.feature_store.feature import Feature

import firebase_admin
from firebase_admin import firestore
from google.cloud.firestore import Client as FirestoreClient
import uuid

from decimal import Decimal
import pandas as pd

from fal.telemetry import telemetry


class FalGeneralException(Exception):
    pass


@dataclass
class DbtTest:
    node: Any
    name: str = field(init=False)
    model: str = field(init=False)
    column: str = field(init=False)
    status: str = field(init=False)

    def __post_init__(self):
        node = self.node
        self.unique_id = node.unique_id
        if node.resource_type == NodeType.Test:
            if hasattr(node, "test_metadata"):
                self.name = node.test_metadata.name

                # node.test_metadata.kwargs looks like this:
                # kwargs={'column_name': 'y', 'model': "{{ get_where_subquery(ref('boston')) }}"}
                # and we want to get 'boston' is the model name that we want extract
                # except in dbt v 0.20.1, where we need to filter 'where' string out
                refs = re.findall(r"'([^']+)'", node.test_metadata.kwargs["model"])
                self.model = [ref for ref in refs if ref != "where"][0]
                self.column = node.test_metadata.kwargs.get("column_name", None)
            else:
                logger.debug(f"Non-generic test was not processed: {node.name}")

    def set_status(self, status: str):
        self.status = status


@dataclass
class DbtModel:
    node: ParsedModelNode
    name: str = field(init=False)
    meta: Dict[str, Any] = field(init=False)
    status: NodeStatus = field(init=False)
    columns: Dict[str, Any] = field(init=False)
    tests: List[DbtTest] = field(init=False)

    def __post_init__(self):
        node = self.node
        self.name = node.name

        # BACKWARDS: Change intorduced in XXX (0.20?)
        # TODO: specify which version is for this
        if hasattr(node.config, "meta"):
            self.meta = node.config.meta
        elif hasattr(node, "meta"):
            self.meta = node.meta

        self.columns = node.columns
        self.unique_id = node.unique_id
        self.tests = []

    def __hash__(self) -> int:
        return self.unique_id.__hash__()

    def get_depends_on_nodes(self) -> List[str]:
        return self.node.depends_on_nodes

    def get_scripts(self, keyword: str, before: bool) -> List[str]:
        # sometimes `scripts` can *be* there and still be None
        if self.meta.get(keyword):
            scripts_node = self.meta[keyword].get("scripts")
            if not scripts_node:
                return []
            if isinstance(scripts_node, list) and before:
                return []
            if before:
                return scripts_node.get("before") or []
            if isinstance(scripts_node, list):
                return scripts_node
            return scripts_node.get("after") or []
        else:
            return []

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

    def get_tests(self) -> List[DbtTest]:
        return list(
            filter(
                lambda test: test.node.resource_type == NodeType.Test,
                map(
                    lambda node: DbtTest(node=node), self.nativeManifest.nodes.values()
                ),
            )
        )

    def get_sources(self) -> List[ParsedSourceDefinition]:
        return list(self.nativeManifest.sources.values())


@dataclass(init=False)
class DbtRunResult:
    nativeRunResult: Optional[RunResultsArtifact]
    results: Sequence[RunResultOutput]

    def __init__(self, nativeRunResult: Optional[RunResultsArtifact]):
        self.results = []
        self.nativeRunResult = nativeRunResult
        if self.nativeRunResult:
            self.results = nativeRunResult.results


@dataclass
class CompileArgs:
    selector_name: Optional[str]
    select: List[str]
    models: List[str]
    exclude: Tuple[str]
    state: Optional[Path]
    single_threaded: Optional[bool]


class WriteModeEnum(Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"


@dataclass(init=False)
class FalDbt:
    project_dir: str
    profiles_dir: str
    scripts_dir: str
    keyword: str
    features: List[Feature]
    method: str
    models: List[DbtModel]
    tests: List[DbtTest]
    el: FalElClient

    _config: RuntimeConfig
    _manifest: DbtManifest
    _run_results: DbtRunResult
    # Could we instead extend it and create a FalRunTak?
    _compile_task: CompileTask
    _state: Optional[Path]
    _global_script_paths: Dict[str, List[str]]

    _firestore_client: Optional[FirestoreClient]

    def __init__(
        self,
        project_dir: str,
        profiles_dir: str,
        select: List[str] = [],
        exclude: Tuple[str] = tuple(),
        selector_name: Optional[str] = None,
        keyword: str = "fal",
        threads: Optional[int] = None,
        state: Optional[Path] = None,
        profile_target: Optional[str] = None,
    ):
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.keyword = keyword
        self._firestore_client = None
        self._state = state

        self.scripts_dir = parse.get_scripts_dir(project_dir)

        lib.initialize_dbt_flags(profiles_dir=profiles_dir)

        self._config = parse.get_dbt_config(project_dir, profiles_dir, threads)

        self._run_results = DbtRunResult(
            parse.get_dbt_results(self.project_dir, self._config)
        )

        self.method = "run"

        if self._run_results.nativeRunResult:
            self.method = self._run_results.nativeRunResult.args["rpc_method"]
            if profile_target is None:
                profile_target = _get_custom_target(self._run_results)

        if profile_target is not None:
            self._config = parse.get_dbt_config(
                project_dir, profiles_dir, threads, profile_target=profile_target
            )

        el_configs = parse.get_el_configs(
            profiles_dir, self._config.profile_name, self._config.target_name
        )

        # Setup EL API clients
        self.el = FalElClient(el_configs)

        # Necessary for manifest loading to not fail
        dbt.tracking.initialize_tracking(profiles_dir)

        args = CompileArgs(selector_name, select, select, exclude, state, None)
        self._compile_task = CompileTask(args, self._config)

        self._compile_task._runtime_initialize()

        self._manifest = DbtManifest(self._compile_task.manifest)

        self.models, self.tests = _map_nodes_to_models(
            self._run_results, self._manifest
        )

        # BACKWARDS: Change intorduced in 1.0.0
        model_paths: List[str] = []
        if hasattr(self._config, "model_paths"):
            model_paths = self._config.model_paths
        elif hasattr(self._config, "source_paths"):
            model_paths = self._config.source_paths
        normalized_model_paths = parse.normalize_paths(project_dir, model_paths)

        self._global_script_paths = parse.get_global_script_configs(
            normalized_model_paths
        )

        self.features = self._find_features()

    @property
    def _profile_target(self):
        return self._config.target_name

    @property
    def threads(self):
        return self._config.threads

    @property
    def target_path(self):
        return self._config.target_path

    @property
    def project_name(self):
        return self._config.project_name

    @telemetry.log_call("list_sources")
    def list_sources(self):
        """
        List tables available for `source` usage, formatting like `[[source_name, table_name], ...]`
        """
        res = []
        for source in self._manifest.get_sources():
            res.append([source.source_name, source.name])

        return res

    @telemetry.log_call("list_models_ids")
    def list_models_ids(self) -> Dict[str, str]:
        """
        List model ids available for `ref` usage, formatting like `[ref_name, ...]`
        """
        res = {}
        for model in self.models:
            res[model.unique_id] = model.status

        return res

    @telemetry.log_call("list_models")
    def list_models(self) -> List[DbtModel]:
        """
        List models
        """
        return self.models

    @telemetry.log_call("list_tests")
    def list_tests(self) -> List[DbtTest]:
        """
        List tests
        """
        return self.tests

    @telemetry.log_call("list_features")
    def list_features(self) -> List[Feature]:
        return self.features

    def _find_features(self) -> List[Feature]:
        """List features defined in schema.yml files."""
        keyword = self.keyword
        models = self.models
        models = list(
            filter(
                # Find models that have both feature store and column defs
                lambda model: keyword in model.meta
                and "feature_store" in model.meta[keyword]
                and len(list(model.columns.keys())) > 0,
                models,
            )
        )
        features = []
        for model in models:
            for column_name in model.columns.keys():
                if column_name == model.meta[keyword]["feature_store"]["entity_column"]:
                    continue
                if (
                    column_name
                    == model.meta[keyword]["feature_store"]["timestamp_column"]
                ):
                    continue
                features.append(
                    Feature(
                        model=model.name,
                        column=column_name,
                        description=model.columns[column_name].description,
                        entity_column=model.meta[keyword]["feature_store"][
                            "entity_column"
                        ],
                        timestamp_column=model.meta[keyword]["feature_store"][
                            "timestamp_column"
                        ],
                    )
                )
        return features

    def _model(
        self, target_model_name: str, target_package_name: Optional[str]
    ) -> ManifestNode:
        target_model: MaybeNonSource = self._manifest.nativeManifest.resolve_ref(
            target_model_name, target_package_name, self.project_dir, self.project_dir
        )

        package_str = f"'{target_package_name}'." if target_package_name else ""
        model_str = f"{package_str}'{target_model_name}'"
        if target_model is None:
            raise Exception(f"Could not find model {model_str}")

        if isinstance(target_model, Disabled):
            raise RuntimeError(f"Model {model_str} is disabled")

        return target_model

    @telemetry.log_call("ref")
    def ref(self, target_1: str, target_2: Optional[str] = None) -> pd.DataFrame:
        """
        Download a dbt model as a pandas.DataFrame automagically.
        """
        target_model_name = target_1
        target_package_name = None
        if target_2 is not None:
            target_package_name = target_1
            target_model_name = target_2

        target_model = self._model(target_model_name, target_package_name)

        result = lib.fetch_target(
            self.project_dir,
            self.profiles_dir,
            target_model,
            profile_target=self._profile_target,
        )
        return pd.DataFrame.from_records(
            result.table.rows, columns=result.table.column_names, coerce_float=True
        )

    def _source(
        self, target_source_name: str, target_table_name: str
    ) -> ParsedSourceDefinition:
        target_source: MaybeParsedSource = self._manifest.nativeManifest.resolve_source(
            target_source_name, target_table_name, self.project_dir, self.project_dir
        )

        if target_source is None:
            raise RuntimeError(
                f"Could not find source '{target_source_name}'.'{target_table_name}'"
            )

        if isinstance(target_source, Disabled):
            raise RuntimeError(
                f"Source '{target_source_name}'.'{target_table_name}' is disabled"
            )

        return target_source

    @telemetry.log_call("source")
    def source(self, target_source_name: str, target_table_name: str) -> pd.DataFrame:
        """
        Download a dbt source as a pandas.DataFrame automagically.
        """

        target_source = self._source(target_source_name, target_table_name)

        result = lib.fetch_target(
            self.project_dir,
            self.profiles_dir,
            target_source,
            profile_target=self._profile_target,
        )
        return pd.DataFrame.from_records(
            result.table.rows, columns=result.table.column_names
        )

    @telemetry.log_call("write_to_source", ["mode"])
    def write_to_source(
        self,
        data: pd.DataFrame,
        target_source_name: str,
        target_table_name: str,
        # TODO: Make dtype named-param in the future?
        dtype: Any = None,
        *,
        mode: str = "append",
    ):
        """
        Write a pandas.DataFrame to a dbt model automagically.
        """

        target_source = self._source(target_source_name, target_table_name)

        write_mode = WriteModeEnum(mode.lower().strip())
        if write_mode == WriteModeEnum.APPEND:
            lib.write_target(
                data,
                self.project_dir,
                self.profiles_dir,
                target_source,
                dtype,
                profile_target=self._profile_target,
            )

        elif write_mode == WriteModeEnum.OVERWRITE:
            lib.overwrite_target(
                data,
                self.project_dir,
                self.profiles_dir,
                target_source,
                dtype,
                profile_target=self._profile_target,
            )

        else:
            raise Exception(f"write_to_source mode `{mode}` not supported")

    @telemetry.log_call("write_to_model", ["mode"])
    def write_to_model(
        self,
        data: pd.DataFrame,
        target_1: str,
        target_2: Optional[str] = None,
        *,
        dtype: Any = None,
        mode: str = "overwrite",
    ):
        """
        Write a pandas.DataFrame to a dbt source automagically.
        """
        target_model_name = target_1
        target_package_name = None
        if target_2 is not None:
            target_package_name = target_1
            target_model_name = target_2

        target_model = self._model(target_model_name, target_package_name)

        write_mode = WriteModeEnum(mode.lower().strip())
        if write_mode == WriteModeEnum.APPEND:
            lib.write_target(
                data,
                self.project_dir,
                self.profiles_dir,
                target_model,
                dtype,
                profile_target=self._profile_target,
            )

        elif write_mode == WriteModeEnum.OVERWRITE:
            lib.overwrite_target(
                data,
                self.project_dir,
                self.profiles_dir,
                target_model,
                dtype,
                profile_target=self._profile_target,
            )

        else:
            raise Exception(f"write_to_model mode `{mode}` not supported")

    @telemetry.log_call("write_to_firestore")
    def write_to_firestore(self, df: pd.DataFrame, collection: str, key_column: str):
        """
        Write a pandas.DataFrame to a GCP Firestore collection. You must specify the column to use as key.
        """

        # Lazily setup Firestore
        self._lazy_setup_firestore()

        if self._firestore_client is None:
            raise FalGeneralException(
                "GCP credentials not setup correctly. Check warnings during initialization."
            )

        df_arr = df.to_dict("records")
        for item in df_arr:
            key = item[key_column]
            data = _firestore_dict_to_document(data=item, key_column=key_column)
            self._firestore_client.collection(collection).document(str(key)).set(data)

    def _lazy_setup_firestore(self):
        if self._firestore_client is not None:
            return

        try:
            from dbt.adapters.bigquery.connections import BigQueryConnectionManager
        except ModuleNotFoundError as not_found:
            raise FalGeneralException(
                "To use firestore, please `pip install dbt-bigquery`"
            ) from not_found

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


def _map_nodes_to_models(run_results: DbtRunResult, manifest: DbtManifest):
    models = manifest.get_models()
    tests = manifest.get_tests()
    status_map = dict(
        map(
            lambda res: [res.unique_id, res.status],
            run_results.results,
        )
    )
    for model in models:
        model.set_status(status_map.get(model.unique_id, NodeStatus.Skipped))
        for test in tests:
            if hasattr(test, "model") and test.model == model.name:
                model.tests.append(test)
                test.set_status(status_map.get(test.unique_id, NodeStatus.Skipped))
                if (
                    test.status != NodeStatus.Skipped
                    and model.status == NodeStatus.Skipped
                ):
                    model.set_status("tested")
    return (models, tests)


def _get_custom_target(run_results: DbtRunResult):
    if "target" in run_results.nativeRunResult.args:
        return run_results.nativeRunResult.args["target"]
    return None
