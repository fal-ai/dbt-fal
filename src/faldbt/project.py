from collections import defaultdict
import os.path
import re
from dataclasses import dataclass, field
from functools import partialmethod
from typing import Dict, Iterable, List, Any, Optional, Tuple, Sequence, Union
from pathlib import Path

from dbt.node_types import NodeType
from dbt.contracts.graph.parsed import ParsedSourceDefinition, TestMetadata
from dbt.contracts.graph.compiled import ManifestNode
from dbt.contracts.graph.manifest import (
    Manifest,
    MaybeNonSource,
    MaybeParsedSource,
    Disabled,
)
from dbt.contracts.results import (
    RunResultsArtifact,
    RunResultOutput,
    NodeStatus,
    FreshnessExecutionResultArtifact,
    FreshnessNodeOutput,
)
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.task.compile import CompileTask
import dbt.tracking

from .lib import WriteModeEnum
from . import parse
from . import lib
from .el_client import FalElClient

from fal.feature_store.feature import Feature

import firebase_admin
from firebase_admin import firestore

import uuid

from decimal import Decimal
import pandas as pd

from fal.telemetry import telemetry


class FalGeneralException(Exception):
    pass


@dataclass
class _DbtNode:
    node: Any = field(repr=False)
    _status: str = field(default=NodeStatus.Skipped.value)

    @property
    def name(self) -> str:
        return self.node.name

    @property
    def unique_id(self) -> str:
        return self.node.unique_id

    def _get_status(self):
        return self._status

    def _set_status(self, status: str):
        self._status = status

    status = property(_get_status, _set_status)


@dataclass
class DbtTest(_DbtNode):
    column: str = field(init=False)
    source: Optional[Tuple[str, str]] = field(init=False, default=None)
    model: Optional[str] = field(init=False, default=None)

    def __post_init__(self):
        node = self.node
        if node.resource_type == NodeType.Test:
            if hasattr(node, "test_metadata"):
                metadata: TestMetadata = self.node.test_metadata

                # metadata.kwargs looks like this:
                # kwargs={'column_name': 'y', 'model': "{{ get_where_subquery(ref('boston')) }}"}
                # and we want to get 'boston' is the model name that we want extract
                # except in dbt v 0.20.1, where we need to filter 'where' string out
                self.column = metadata.kwargs.get("column_name", None)

                # TODO: Handle package models?
                refs = re.findall(r"ref\('([^']+)'\)", metadata.kwargs["model"])

                if refs:
                    self.model = refs[0]

                sources = re.findall(
                    r"source\('([^']+)', *'([^']+)'\)", metadata.kwargs["model"]
                )
                if sources:
                    self.source = sources[0]
            else:
                logger.warn(f"Non-generic test was not processed: {node.name}")

    @property
    def name(self) -> str:
        metadata: TestMetadata = self.node.test_metadata
        return metadata.name


@dataclass
class _DbtTestableNode(_DbtNode):
    tests: List[DbtTest] = field(default_factory=list)

    def _get_status(self):
        if self._status == NodeStatus.Skipped and any(
            test.status != NodeStatus.Skipped for test in self.tests
        ):
            return "tested"
        else:
            return self._status

    status = property(_get_status, _DbtNode._set_status)


@dataclass
class DbtSource(_DbtTestableNode):
    freshness: Optional[FreshnessNodeOutput] = field(default=None)

    def __repr__(self):
        attrs = ["name", "tests", "status"]
        props = ", ".join([f"{item}={repr(getattr(self, item))}" for item in attrs])
        return f"DbtSource({props})"

    @property
    def table_name(self) -> str:
        return self.node.name

    @property
    def name(self) -> str:
        return self.node.source_name


@dataclass
class DbtModel(_DbtTestableNode):
    python_model: Optional[Path] = field(default=None)

    def __repr__(self):
        attrs = ["name", "alias", "unique_id", "columns", "tests", "status"]
        props = ", ".join([f"{item}={repr(getattr(self, item))}" for item in attrs])
        return f"DbtModel({props})"

    @property
    def columns(self):
        return self.node.columns

    @property
    def alias(self):
        return self.node.alias

    @property
    def meta(self):
        # BACKWARDS: Change intorduced in XXX (0.20?)
        # TODO: specify which version is for this
        if hasattr(self.node.config, "meta"):
            return self.node.config.meta
        elif hasattr(self.node, "meta"):
            return self.node.meta

    def __hash__(self) -> int:
        return self.unique_id.__hash__()

    def get_depends_on_nodes(self) -> List[str]:
        return self.node.depends_on_nodes

    def _get_hook_paths(self, hook_type: str, keyword: str = "fal") -> List[str]:
        meta = self.meta or {}

        keyword_dict = meta.get(keyword) or {}
        if not isinstance(keyword_dict, dict):
            return []

        hooks = keyword_dict.get(hook_type) or []
        if not isinstance(hooks, list):
            return []

        return hooks

    get_pre_hook_paths = partialmethod(_get_hook_paths, hook_type="pre-hook")
    get_post_hook_paths = partialmethod(_get_hook_paths, hook_type="post-hook")

    def get_scripts(self, keyword: str, *, before: bool) -> List[str]:
        # sometimes properties can *be* there and still be None
        meta = self.meta or {}

        keyword_dict = meta.get(keyword) or {}
        if not isinstance(keyword_dict, dict):
            return []

        scripts_node = keyword_dict.get("scripts") or []
        if not scripts_node:
            return []

        if isinstance(scripts_node, list):
            if before:
                return []
            else:
                return scripts_node

        if not isinstance(scripts_node, dict):
            return []

        if before:
            return scripts_node.get("before") or []
        else:
            return scripts_node.get("after") or []


@dataclass
class DbtRunResult:
    nativeRunResult: Optional[RunResultsArtifact]

    @property
    def results(self) -> Sequence[RunResultOutput]:
        if self.nativeRunResult:
            return self.nativeRunResult.results
        else:
            return []


@dataclass
class DbtFreshnessExecutionResult:
    _artifact: Optional[FreshnessExecutionResultArtifact]

    @property
    def results(self) -> Sequence[FreshnessNodeOutput]:
        if self._artifact:
            return self._artifact.results
        else:
            return []


@dataclass
class DbtManifest:
    nativeManifest: Manifest

    def get_model_nodes(self) -> Iterable[ManifestNode]:
        return (
            node
            for node in self.nativeManifest.nodes.values()
            if node.resource_type == NodeType.Model
        )

    def get_test_nodes(self) -> Iterable[ManifestNode]:
        return (
            node
            for node in self.nativeManifest.nodes.values()
            if node.resource_type == NodeType.Test
        )

    def get_source_nodes(self) -> Iterable[ParsedSourceDefinition]:
        return self.nativeManifest.sources.values()

    def _map_nodes(
        self,
        run_results: DbtRunResult,
        freshness_results: DbtFreshnessExecutionResult,
        generated_models: Dict[str, Path],
    ) -> Tuple[List[DbtModel], List[DbtSource], List[DbtTest]]:
        status_map = {r.unique_id: r.status for r in run_results.results}

        tests: List[DbtTest] = []
        tests_dict: Dict[Union[Tuple[str, str], str], List[DbtTest]] = defaultdict(list)
        for node in self.get_test_nodes():
            test = DbtTest(
                node=node,
                _status=status_map.get(node.unique_id, NodeStatus.Skipped).value,
            )
            tests.append(test)

            if test.model:
                tests_dict[test.model].append(test)
            if test.source:
                tests_dict[test.source].append(test)

        models: List[DbtModel] = []
        for node in self.get_model_nodes():
            model = DbtModel(
                node=node,
                _status=status_map.get(node.unique_id, NodeStatus.Skipped).value,
                tests=tests_dict[node.name],
                python_model=generated_models.get(node.name),
            )
            models.append(model)

        source_freshness_map = {r.unique_id: r for r in freshness_results.results}

        sources: List[DbtSource] = []
        for node in self.get_source_nodes():
            source = DbtSource(
                node=node,
                _status=status_map.get(node.unique_id, NodeStatus.Skipped).value,
                tests=tests_dict[(node.source_name, node.name)],
                freshness=source_freshness_map.get(node.unique_id),
            )
            sources.append(source)

        return models, sources, tests


@dataclass
class CompileArgs:
    selector_name: Optional[str]
    select: List[str]
    models: List[str]
    exclude: Tuple[str]
    state: Optional[Path]
    single_threaded: Optional[bool]


class FalDbt:
    """Holds the entire dbt project information."""

    # TODO: figure out a meaningful __repr__ for this class
    def __init__(
        self,
        project_dir: str,
        profiles_dir: str,
        select: List[str] = [],
        exclude: Tuple[str] = tuple(),
        selector_name: Optional[str] = None,
        keyword: str = "fal",
        threads: Optional[int] = None,
        state: Optional[str] = None,
        profile_target: Optional[str] = None,
        args_vars: str = "{}",
        generated_models: Dict[str, Path] = {},
    ):
        if not lib.IS_DBT_V1PLUS:
            raise NotImplementedError(
                f"dbt version {lib.DBT_VCURRENT} is no longer supported, please upgrade to dbt 1.0.0 or above"
            )

        self.project_dir = os.path.realpath(os.path.expanduser(project_dir))
        self.profiles_dir = os.path.realpath(os.path.expanduser(profiles_dir))
        self.keyword = keyword
        self._firestore_client = None
        self._state = None
        if state is not None:
            self._state = Path(os.path.realpath(os.path.expanduser(state)))

        self.scripts_dir = parse.get_scripts_dir(self.project_dir, args_vars)

        lib.initialize_dbt_flags(profiles_dir=self.profiles_dir)

        # Can be overwritten if profile_target is not None
        self._config = parse.get_dbt_config(
            project_dir=self.project_dir,
            profiles_dir=self.profiles_dir,
            threads=threads,
        )

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
                project_dir=self.project_dir,
                profiles_dir=self.profiles_dir,
                threads=threads,
                profile_target=profile_target,
            )

        lib.register_adapters(self._config)

        el_configs = parse.get_el_configs(
            self.profiles_dir, self._config.profile_name, self._config.target_name
        )

        # Setup EL API clients
        self.el = FalElClient(el_configs)

        # Necessary for manifest loading to not fail
        dbt.tracking.initialize_tracking(self.profiles_dir)

        args = CompileArgs(selector_name, select, select, exclude, self._state, None)
        self._compile_task = CompileTask(args, self._config)

        self._compile_task._runtime_initialize()

        self._manifest = DbtManifest(self._compile_task.manifest)

        freshness_execution_results = DbtFreshnessExecutionResult(
            parse.get_dbt_sources_artifact(self.project_dir, self._config)
        )

        self.models, self.sources, self.tests = self._manifest._map_nodes(
            self._run_results,
            freshness_execution_results,
            generated_models,
        )

        normalized_model_paths = parse.normalize_paths(
            self.project_dir, self.source_paths
        )

        self._global_script_paths = parse.get_global_script_configs(
            normalized_model_paths
        )

        self.features = self._find_features()

    @property
    def source_paths(self) -> List[str]:
        # BACKWARDS: Change intorduced in 1.0.0
        if hasattr(self._config, "model_paths"):
            return self._config.model_paths
        elif hasattr(self._config, "source_paths"):
            return self._config.source_paths  # type: ignore
        else:
            raise RuntimeError("No model_paths in config")

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
        List tables available for `source` usage
        """
        return self.sources

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
                and isinstance(model.meta[keyword], dict)
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

        return lib.fetch_target(
            self.project_dir,
            self.profiles_dir,
            target_model,
            self._profile_target,
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

        return lib.fetch_target(
            self.project_dir,
            self.profiles_dir,
            target_source,
            self._profile_target,
        )

    @telemetry.log_call("write_to_source", ["mode"])
    def write_to_source(
        self,
        data: pd.DataFrame,
        target_source_name: str,
        target_table_name: str,
        *,
        dtype: Any = None,
        mode: str = "append",
    ):
        """
        Write a pandas.DataFrame to a dbt source automagically.
        """

        target_source = self._source(target_source_name, target_table_name)

        write_mode = WriteModeEnum(mode.lower().strip())
        if write_mode == WriteModeEnum.APPEND:
            lib.write_target(
                data,
                self.project_dir,
                self.profiles_dir,
                self._profile_target,
                target_source,
                dtype=dtype,
            )

        elif write_mode == WriteModeEnum.OVERWRITE:
            lib.overwrite_target(
                data,
                self.project_dir,
                self.profiles_dir,
                self._profile_target,
                target_source,
                dtype=dtype,
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
        Write a pandas.DataFrame to a dbt model automagically.
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
                self._profile_target,
                target_model,
                dtype=dtype,
            )

        elif write_mode == WriteModeEnum.OVERWRITE:
            lib.overwrite_target(
                data,
                self.project_dir,
                self.profiles_dir,
                self._profile_target,
                target_model,
                dtype=dtype,
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

    @telemetry.log_call("execute_sql")
    def execute_sql(self, sql: str) -> pd.DataFrame:
        """Execute a sql query."""

        # HACK: we need to pass config in because of weird behavior of execute_sql when
        # ran from GitHub Actions. For some reason, it can not find the right profile.
        # Haven't been able to reproduce this behavior locally and therefore developed
        # this workaround.
        compiled_result = lib.compile_sql(
            self.project_dir,
            self.profiles_dir,
            self._profile_target,
            sql,
            config=self._config,
        )

        # HACK: we need to pass config in because of weird behavior of execute_sql when
        # ran from GitHub Actions. For some reason, it can not find the right profile.
        # Haven't been able to reproduce this behavior locally and therefore developed
        # this workaround.
        return lib.execute_sql(
            self.project_dir,
            self.profiles_dir,
            self._profile_target,
            compiled_result.compiled_sql,  # type: ignore
            config=self._config,
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


def _get_custom_target(run_results: DbtRunResult):
    if "target" in run_results.nativeRunResult.args:
        return run_results.nativeRunResult.args["target"]
    return None
