from collections import defaultdict
import os.path
from dataclasses import dataclass, field
from typing import (
    Dict,
    Iterable,
    List,
    Any,
    Optional,
    Tuple,
    Sequence,
    TYPE_CHECKING,
)
from pathlib import Path
from deprecation import deprecated

import fal.dbt.integration.version as version

from dbt.cli.resolvers import default_profiles_dir
from dbt.cli.main import dbtRunner, dbtRunnerResult

from dbt.contracts.graph.nodes import (
    SourceDefinition,
    TestMetadata,
    GenericTestNode,
    SingularTestNode,
)
from dbt.contracts.graph.nodes import ManifestNode

from dbt.contracts.graph.manifest import (
    Manifest,
    MaybeNonSource,
    MaybeParsedSource,
    Disabled,
)

from dbt.node_types import NodeType
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.results import (
    RunResultsArtifact,
    RunResultOutput,
    NodeStatus,
    FreshnessExecutionResultArtifact,
    FreshnessNodeOutput,
)
from dbt.task.compile import CompileTask

from . import parse
from . import lib
from . import version

from fal.dbt.feature_store.feature import Feature

import pandas as pd

from fal.dbt.telemetry import telemetry
from fal.dbt.utils import has_side_effects

if TYPE_CHECKING:
    from fal.dbt.fal_script import Hook, TimingType
    from fal.dbt.packages.environments import BaseEnvironment


class FalGeneralException(Exception):
    pass


FAL = "fal"


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
    model_ids: List[str] = field(init=False, default_factory=list)
    source_ids: List[str] = field(init=False, default_factory=list)

    @classmethod
    def init(cls, node):
        if node.resource_type == NodeType.Test:
            if isinstance(node, GenericTestNode):
                test = DbtGenericTest(node=node)
            elif isinstance(node, SingularTestNode):
                test = DbtSingularTest(node=node)
            else:
                raise ValueError(f"Unexpected test class {node.__class__.__name__}")

            for dep in test.node.depends_on.nodes:
                if dep.startswith("model."):
                    test.model_ids.append(dep)
                if dep.startswith("source."):
                    test.source_ids.append(dep)

            return test
        else:
            raise TypeError(
                f"Initialized DbtTest with node of type {node.resource_type}"
            )


@dataclass
class DbtGenericTest(DbtTest):
    column: Optional[str] = field(init=False)

    def __repr__(self):
        attrs = ["name", "_status", "model_ids", "source_ids", "column"]
        props = ", ".join([f"{item}={repr(getattr(self, item))}" for item in attrs])
        return f"DbtGenericTest({props})"

    def __post_init__(self):
        assert isinstance(self.node, GenericTestNode)
        self.column = self.node.column_name

        # Column name might be stored in test_metadata
        if not self.column and self.node.test_metadata.kwargs.get("column_name"):
            self.column = self.node.test_metadata.kwargs.get("column_name")

    @property
    def source_id(self):
        if self.source_ids:
            return self.source_ids[0]

    @property
    def model_id(self):
        if self.model_ids:
            return self.model_ids[0]

    # TODO: Deprecate?
    @property
    def source(self):
        if self.source_id:
            parts = self.source_id.split(".")
            return parts[-2], parts[-1]

    # TODO: Deprecate?
    @property
    def model(self):
        if self.model_id:
            # TODO: handle package models
            parts = self.model_id.split(".")
            return parts[-1]

    @property
    def name(self) -> str:
        metadata: TestMetadata = self.node.test_metadata
        return metadata.name


@dataclass
class DbtSingularTest(DbtTest):
    def __post_init__(self):
        assert isinstance(self.node, SingularTestNode)


@dataclass
class _DbtTestableNode(_DbtNode):
    # TODO: should this include singular tests that ref to this node?
    tests: List[DbtGenericTest] = field(default_factory=list)

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
        attrs = ["name", "table_name", "tests", "status"]
        props = ", ".join([f"{item}={repr(getattr(self, item))}" for item in attrs])
        return f"DbtSource({props})"

    @property
    def meta(self):
        return self.node.meta

    @property
    def table_name(self) -> str:
        return self.node.name

    @property
    def name(self) -> str:
        return self.node.source_name


@dataclass
class DbtModel(_DbtTestableNode):
    python_model: Optional[Path] = field(default=None)

    _adapter_response: Optional[AdapterResponse] = field(default=None)

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
        return self.node.meta

    @property
    def is_incremental(self):
        return self.node.config.materialized == "incremental"

    def _get_adapter_response(self):
        return self._adapter_response

    def _set_adapter_response(self, adapter_response: Optional[dict]):
        self._adapter_response = (
            AdapterResponse.from_dict(adapter_response) if adapter_response else None
        )

    adapter_response = property(_get_adapter_response, _set_adapter_response)

    def __hash__(self) -> int:
        return self.unique_id.__hash__()

    def get_depends_on_nodes(self) -> List[str]:
        return self.node.depends_on_nodes

    def get_hooks(
        self,
        hook_type: "TimingType",
    ) -> List["Hook"]:
        from fal.dbt.fal_script import create_hook, TimingType

        meta = self.meta or {}

        keyword_dict = meta.get(FAL) or {}
        if not isinstance(keyword_dict, dict):
            return []

        if hook_type == TimingType.PRE:
            hook_key = "pre-hook"
        elif hook_type == TimingType.POST:
            hook_key = "post-hook"
        else:
            raise ValueError(f"Unexpected hook type {hook_type}")

        raw_hooks = keyword_dict.get(hook_key) or []
        if not isinstance(raw_hooks, list):
            return []

        return [
            create_hook(raw_hook, default_environment_name=self.environment_name)
            for raw_hook in raw_hooks
        ]

    def get_scripts(self, *, before: bool) -> List[str]:
        # sometimes properties can *be* there and still be None
        meta = self.meta or {}

        keyword_dict = meta.get(FAL) or {}
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

    @property
    def environment_name(self) -> Optional[str]:
        meta = self.meta or {}
        fal = meta.get("fal") or {}
        return fal.get("environment")


@dataclass
class DbtRunResult:
    native_run_result: Optional[RunResultsArtifact]

    @property
    @deprecated(details="Use native_run_result instead")
    def nativeRunResult(self):
        return self.native_run_result

    @property
    def results(self) -> Sequence[RunResultOutput]:
        if self.native_run_result:
            return self.native_run_result.results
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
    native_manifest: Manifest

    @property
    @deprecated(details="Use native_manifest instead")
    def nativeManifest(self):
        return self.native_manifest

    def get_model_nodes(self) -> Iterable[ManifestNode]:
        return (
            node
            for node in self.native_manifest.nodes.values()
            if node.resource_type == NodeType.Model
        )

    def get_test_nodes(self) -> Iterable[ManifestNode]:
        return (
            node
            for node in self.native_manifest.nodes.values()
            if node.resource_type == NodeType.Test
        )

    def get_source_nodes(self) -> Iterable[SourceDefinition]:
        return self.native_manifest.sources.values()

    def _map_nodes(
        self,
        run_results: DbtRunResult,
        freshness_results: DbtFreshnessExecutionResult,
        generated_models: Dict[str, Path],
    ) -> Tuple[List[DbtModel], List[DbtSource], List[DbtTest]]:
        results_map = {r.unique_id: r for r in run_results.results}

        tests: List[DbtTest] = []

        tests_dict: Dict[str, List[DbtGenericTest]] = defaultdict(list)
        for node in self.get_test_nodes():
            test: DbtTest = DbtTest.init(node=node)

            result = results_map.get(node.unique_id)
            if result:
                test.status = result.status.value

            tests.append(test)

            if isinstance(test, DbtGenericTest):
                if test.model_id:
                    tests_dict[test.model_id].append(test)
                if test.source_id:
                    tests_dict[test.source_id].append(test)

        models: List[DbtModel] = []
        for node in self.get_model_nodes():
            model = DbtModel(
                node=node,
                tests=tests_dict[node.unique_id],
                python_model=generated_models.get(node.name),
            )

            result = results_map.get(node.unique_id)
            if result:
                model.status = result.status.value
                model.adapter_response = result.adapter_response

            models.append(model)

        source_freshness_map = {r.unique_id: r for r in freshness_results.results}

        sources: List[DbtSource] = []
        for node in self.get_source_nodes():
            source = DbtSource(
                node=node,
                tests=tests_dict[node.unique_id],
                freshness=source_freshness_map.get(node.unique_id),
            )

            result = results_map.get(node.unique_id)
            if result:
                source.status = result.status.value

            sources.append(source)

        return models, sources, tests


@dataclass
class CompileArgs:
    selector: Optional[str]
    select: List[str]
    models: List[str]
    exclude: Tuple[str]
    state: Optional[Path]
    single_threaded: Optional[bool]


@has_side_effects
class FalDbt:
    """Holds the entire dbt project information."""

    # TODO: figure out a meaningful __repr__ for this class
    def __init__(
        self,
        project_dir: Optional[str] = None,
        profiles_dir: Optional[str] = None,
        select: List[str] = [],
        exclude: Tuple[str] = tuple(),
        selector: Optional[str] = None,
        threads: Optional[int] = None,
        state: Optional[str] = None,
        profile_target: Optional[str] = None,
        args_vars: str = "{}",
        generated_models: Dict[str, Path] = {},
    ):
        if not version.is_version_plus("1.0.0"):
            raise NotImplementedError(
                f"dbt version {version.DBT_VCURRENT} is no longer supported, please upgrade to dbt 1.0.0 or above"
            )

        if project_dir is None:
            project_dir = os.getcwd()

        if profiles_dir is None:
            profiles_dir = str(default_profiles_dir())

        project_dir = os.path.realpath(os.path.expanduser(project_dir))
        profiles_dir = os.path.realpath(os.path.expanduser(profiles_dir))

        vars = parse.parse_cli_vars(args_vars)

        flags = lib.initialize_dbt_flags(
            profiles_dir=profiles_dir,
            project_dir=project_dir,
            threads=threads,
            profile_target=profile_target,
            vars=vars,
        )

        self.project_dir = flags.PROJECT_DIR
        self.profiles_dir = flags.PROFILES_DIR

        self._state = None
        if state is not None:
            self._state = Path(os.path.realpath(os.path.expanduser(state)))

        self.scripts_dir = parse.get_scripts_dir(self.project_dir, args_vars)


        # Can be overwritten if profile_target is not None
        self._config = parse.get_dbt_config(
            project_dir=self.project_dir,
            profiles_dir=self.profiles_dir,
            profile_target=profile_target,
            threads=threads,
            args_vars=args_vars,
        )

        self._run_results = DbtRunResult(
            parse.get_dbt_results(self.project_dir, self._config)
        )

        if self._run_results.native_run_result:
            if profile_target is None:
                profile_target = _get_custom_target(self._run_results)

        if profile_target is not None:
            self._config = parse.get_dbt_config(
                project_dir=self.project_dir,
                profiles_dir=self.profiles_dir,
                threads=threads,
                profile_target=profile_target,
                args_vars=args_vars,
            )

        lib.register_adapters(self._config)

        parse_result = self._dbt_invoke("parse")
        native_manifest: Manifest = parse_result.result # type: ignore

        # Necessary for manifest loading to not fail
        # dbt.tracking.initialize_tracking(self.profiles_dir)

        args = CompileArgs(selector, select, select, exclude, self._state, None)
        self._compile_task = CompileTask(args, self._config, native_manifest)

        self._compile_task._runtime_initialize()

        self._manifest = DbtManifest(native_manifest)

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
        self._environments = None

        telemetry.log_api(
            action="faldbt_initialized",
            dbt_config=self._config,
        )

    def _dbt_invoke(
        self, cmd: str, args: Optional[List[str]] = None
    ) -> dbtRunnerResult:
        runner = dbtRunner()

        if args is None:
            args = []

        project_args = [
            "--project-dir",
            self.project_dir,
            "--profiles-dir",
            self.profiles_dir,
            "--target",
            self._profile_target,
        ]

        # TODO: Intervene the dbt logs and capture them to avoid printing them to the console
        return runner.invoke([cmd] + project_args + args)

    @property
    def model_paths(self) -> List[str]:
        return self._config.model_paths

    @property
    @deprecated(details="Use model_paths instead")
    def source_paths(self) -> List[str]:
        return self.model_paths

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

    def list_sources(self) -> List[DbtSource]:
        """
        List tables available for `source` usage
        """
        with telemetry.log_time("list_sources", dbt_config=self._config):
            return self.sources

    def list_models_ids(self) -> Dict[str, str]:
        """
        List model ids available for `ref` usage, formatting like `[ref_name, ...]`
        """
        with telemetry.log_time("list_models_ids", dbt_config=self._config):
            res = {}
            for model in self.models:
                res[model.unique_id] = model.status

            return res

    def list_models(self) -> List[DbtModel]:
        """
        List models
        """
        with telemetry.log_time("list_models", dbt_config=self._config):
            return self.models

    def list_tests(self) -> List[DbtTest]:
        """
        List tests
        """
        with telemetry.log_time("list_tests", dbt_config=self._config):
            return self.tests

    def list_features(self) -> List[Feature]:
        with telemetry.log_time("list_features", dbt_config=self._config):
            return self.features

    def _find_features(self) -> List[Feature]:
        """List features defined in schema.yml files."""
        models = self.models
        models = list(
            filter(
                # Find models that have both feature store and column defs
                lambda model: FAL in model.meta
                and isinstance(model.meta[FAL], dict)
                and "feature_store" in model.meta[FAL]
                and len(list(model.columns.keys())) > 0,
                models,
            )
        )
        features = []
        for model in models:
            for column_name in model.columns.keys():
                if column_name == model.meta[FAL]["feature_store"]["entity_column"]:
                    continue
                if column_name == model.meta[FAL]["feature_store"]["timestamp_column"]:
                    continue
                features.append(
                    Feature(
                        model=model.name,
                        column=column_name,
                        description=model.columns[column_name].description,
                        entity_column=model.meta[FAL]["feature_store"]["entity_column"],
                        timestamp_column=model.meta[FAL]["feature_store"][
                            "timestamp_column"
                        ],
                    )
                )
        return features

    def _model(
        self, target_model_name: str, target_package_name: Optional[str]
    ) -> ManifestNode:
        # HACK: always setting node package as self.project_dir
        target_model: MaybeNonSource = self._manifest.native_manifest.resolve_ref(
            target_model_name,
            target_package_name,
            None,
            self.project_dir,
            self.project_dir,
        )
        package_str = f"'{target_package_name}'." if target_package_name else ""
        model_str = f"{package_str}'{target_model_name}'"
        if target_model is None:
            raise Exception(f"Could not find model {model_str}")

        if isinstance(target_model, Disabled):
            raise RuntimeError(f"Model {model_str} is disabled")

        return target_model

    def ref(self, target_1: str, target_2: Optional[str] = None) -> pd.DataFrame:
        """
        Download a dbt model as a pandas.DataFrame automagically.
        """
        with telemetry.log_time("ref", dbt_config=self._config):
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
                config=self._config,
            )

    def _source(
        self, target_source_name: str, target_table_name: str
    ) -> SourceDefinition:
        # HACK: always setting node package as self.project_dir
        target_source: MaybeParsedSource = (
            self._manifest.native_manifest.resolve_source(
                target_source_name,
                target_table_name,
                self.project_dir,
                self.project_dir,
            )
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

    def source(self, target_source_name: str, target_table_name: str) -> pd.DataFrame:
        """
        Download a dbt source as a pandas.DataFrame automagically.
        """
        with telemetry.log_time("source", dbt_config=self._config):
            target_source = self._source(target_source_name, target_table_name)

            return lib.fetch_target(
                self.project_dir,
                self.profiles_dir,
                target_source,
                self._profile_target,
                config=self._config,
            )

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

        with telemetry.log_time(
            "write_to_source",
            dbt_config=self._config,
            additional_props={"args": {"mode": mode}},
        ):
            target_source = self._source(target_source_name, target_table_name)

            write_mode = lib.WriteModeEnum(mode.lower().strip())
            if write_mode == lib.WriteModeEnum.APPEND:
                lib.write_target(
                    data,
                    self.project_dir,
                    self.profiles_dir,
                    self._profile_target,
                    target_source,
                    dtype=dtype,
                    config=self._config,
                )

            elif write_mode == lib.WriteModeEnum.OVERWRITE:
                lib.overwrite_target(
                    data,
                    self.project_dir,
                    self.profiles_dir,
                    self._profile_target,
                    target_source,
                    dtype=dtype,
                    config=self._config,
                )

            else:
                raise Exception(f"write_to_source mode `{mode}` not supported")

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

        with telemetry.log_time(
            "write_to_model",
            dbt_config=self._config,
            additional_props={"args": {"mode": mode}},
        ):
            target_model_name = target_1
            target_package_name = None
            if target_2 is not None:
                target_package_name = target_1
                target_model_name = target_2

            target_model = self._model(target_model_name, target_package_name)

            write_mode = lib.WriteModeEnum(mode.lower().strip())
            if write_mode == lib.WriteModeEnum.APPEND:
                lib.write_target(
                    data,
                    self.project_dir,
                    self.profiles_dir,
                    self._profile_target,
                    target_model,
                    dtype=dtype,
                    config=self._config,
                )

            elif write_mode == lib.WriteModeEnum.OVERWRITE:
                lib.overwrite_target(
                    data,
                    self.project_dir,
                    self.profiles_dir,
                    self._profile_target,
                    target_model,
                    dtype=dtype,
                    config=self._config,
                )

            else:
                raise Exception(f"write_to_model mode `{mode}` not supported")

    def execute_sql(self, sql: str) -> pd.DataFrame:
        """Execute a sql query."""

        with telemetry.log_time("execute_sql", dbt_config=self._config):
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

            # NOTE: changed in version 1.3.0 to `compiled_code`
            if hasattr(compiled_result, "compiled_code"):
                sql = compiled_result.compiled_code
            else:
                sql = compiled_result.compiled_sql
            return lib.execute_sql(
                self.project_dir,
                self.profiles_dir,
                self._profile_target,
                sql,
                config=self._config,
            )

    def _load_environment(self, name: str) -> "BaseEnvironment":
        """
        Return the environment for the given ``name``.
        If the environment does not exist, it raises an exception.
        """
        if self._environments is None:
            self._environments = parse.load_environments(self.project_dir)
        return self._environments[name]


def _get_custom_target(run_results: DbtRunResult):
    if "target" in run_results.native_run_result.args:
        return run_results.native_run_result.args["target"]
    return None
