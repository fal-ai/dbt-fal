from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.base.meta import AdapterMeta, available
from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.connection import AdapterResponse

from dbt.fal.adapters.teleport.info import (
    TeleportInfo,
    S3TeleportInfo,
    LocalTeleportInfo,
)
from dbt.fal.adapters.teleport.impl import TeleportAdapter
from dbt.fal.adapters.python.impl import PythonAdapter
from dbt.parser.manifest import MacroManifest, Manifest, ManifestLoader

from . import telemetry

from .connections import FalConnectionManager, FalCredentials, TeleportTypeEnum

from .teleport_adapter_support import wrap_db_adapter
from .teleport import DataLocation, run_in_environment_with_teleport, run_with_teleport

from .adapter_support import reload_adapter_cache
from .adapter import run_in_environment_with_adapter, run_with_adapter

from .utils.environments import fetch_environment, db_adapter_config


class FalAdapterMixin(TeleportAdapter, metaclass=AdapterMeta):
    ConnectionManager = FalConnectionManager

    def __init__(self, config, db_adapter: BaseAdapter):
        self.config = config
        self._db_adapter = db_adapter

        self._relation_data_location_cache: DataLocation = DataLocation({})
        if self.is_teleport():
            self._wrapper = wrap_db_adapter(self._db_adapter, self.credentials.teleport)

    @classmethod
    def type(cls):
        return "fal_experimental"

    @classmethod
    def storage_formats(cls):
        return ["csv", "parquet"]

    @available
    def is_teleport(self) -> bool:
        return getattr(self.credentials, "teleport", None) is not None

    @property
    def manifest(self) -> Manifest:
        return ManifestLoader.get_full_manifest(self.config)

    @property
    def macro_manifest(self) -> MacroManifest:
        return self._db_adapter.load_macro_manifest()


    @telemetry.log_call("experimental_submit_python_job", config=True)
    def submit_python_job(
        self, parsed_model: dict, compiled_code: str
    ) -> AdapterResponse:
        """Execute the given `compiled_code` in the target environment."""
        environment_name = parsed_model["config"].get(
            "fal_environment",
            self.credentials.default_environment,
        )

        machine_type = parsed_model["config"].get(
            "fal_machine",
            "S",
        )

        environment, is_local = fetch_environment(
            self.config.project_root,
            environment_name,
            machine_type,
            self.credentials
        )

        telemetry.log_api(
            "experimental_submit_python_job_config",
            config=self.config,
            additional_props={
                "is_teleport": self.is_teleport(),
                "environment_is_local": is_local,
            },
        )

        if self.is_teleport():
            # We need to build teleport_info because we read from the external storage,
            # we did not _localize_ the data in `teleport_from_external_storage`
            teleport_info = self._build_teleport_info()
            if is_local:
                result_table_path = run_with_teleport(
                    compiled_code,
                    teleport_info=teleport_info,
                    locations=self._relation_data_location_cache,
                    config=db_adapter_config(self.config)
                )
            else:
                result_table_path = run_in_environment_with_teleport(
                    environment,
                    compiled_code,
                    teleport_info=teleport_info,
                    locations=self._relation_data_location_cache,
                    config=db_adapter_config(self.config),
                    adapter_type=self._db_adapter.type()
                )

            relation = self._db_adapter.Relation.create(
                parsed_model["database"], parsed_model["schema"], parsed_model["alias"]
            )
            self._sync_result_table(relation)

            return AdapterResponse("OK")

        else:
            if is_local:
                return run_with_adapter(compiled_code, self._db_adapter, self.config)

            with self._invalidate_db_cache():
                return run_in_environment_with_adapter(
                    environment,
                    compiled_code,
                    db_adapter_config(self.config),
                    self.manifest,
                    self.macro_manifest,
                    self._db_adapter.type()
                )

    @contextmanager
    def _invalidate_db_cache(self) -> Iterator[None]:
        try:
            yield
        finally:
            # Since executed Python code might alter the database
            # layout, we need to regenerate the relations cache
            # after every time we execute a Python model.
            #
            # TODO: maybe propagate a list of tuples with the changes
            # from the Python runner, so that we can tell the cache
            # manager about what is going on instead of hard-resetting
            # the cache-db.
            reload_adapter_cache(self._db_adapter, self.manifest)

    @property
    def credentials(self):
        python_creds: FalCredentials = self.config.python_adapter_credentials
        # dbt-fal is not configured as a Python adapter,
        # maybe we should raise an error?
        assert python_creds is not None
        return python_creds

    def teleport_from_external_storage(
        self, relation: BaseRelation, relation_path: str, teleport_info: TeleportInfo
    ):
        """
        Store the teleport urls for later use
        """

        rel_name = teleport_info.relation_name(relation)
        self._relation_data_location_cache[rel_name] = relation_path

    def teleport_to_external_storage(
        self, relation: BaseRelation, teleport_info: TeleportInfo
    ):
        # Already in external_storage, we do not have local storage
        # Just return the path
        return teleport_info.build_relation_path(relation)

    # TODO: cache this?
    def _build_teleport_info(self):
        teleport_creds = self.credentials.teleport
        assert teleport_creds

        teleport_format = TeleportAdapter.find_format(self, self._wrapper)

        if teleport_creds.type == TeleportTypeEnum.LOCAL:
            assert teleport_creds.local_path
            return LocalTeleportInfo(
                teleport_format, teleport_creds, teleport_creds.local_path
            )
        elif teleport_creds.type == TeleportTypeEnum.REMOTE_S3:
            assert teleport_creds.s3_bucket
            return S3TeleportInfo(
                teleport_format, teleport_creds, teleport_creds.s3_bucket, "teleport"
            )
        else:
            raise NotImplementedError(
                f"Teleport credentials of type {teleport_creds.type} not supported"
            )

    ######
    # HACK: Following implementations only necessary until dbt-core adds Teleport.
    #####
    @available
    def sync_teleport_relation(self, relation: BaseRelation):
        """
        Internal implementation of sync to avoid dbt-core changes
        """
        teleport_info = self._build_teleport_info()
        data_path = self._wrapper.teleport_to_external_storage(relation, teleport_info)
        self.teleport_from_external_storage(relation, data_path, teleport_info)

    def _sync_result_table(self, relation: BaseRelation):
        """
        Internal implementation of sync to put data back into datawarehouse.
        This is necessary because Teleport is not part of dbt-core.
        Once it is and adapters implement it, we will sync the result table back.
        Instead the other adapter will call `sync_teleport` and it will automatically call
        FalAdapter's `teleport_to_external_storage` and the adapter's `teleport_from_external_storage`.
        """
        teleport_info = self._build_teleport_info()
        data_path = self.teleport_to_external_storage(relation, teleport_info)
        self._wrapper.teleport_from_external_storage(relation, data_path, teleport_info)


class FalAdapter(FalAdapterMixin, PythonAdapter):
    def __init__(self, config):
        PythonAdapter.__init__(self, config)
        FalAdapterMixin.__init__(self, config, self._db_adapter)

        telemetry.log_api(
            "experimental_init",
            config=config,
            additional_props={"is_teleport": self.is_teleport()},
        )

    @classmethod
    def is_cancelable(cls) -> bool:
        # TODO: maybe it is?
        return False
