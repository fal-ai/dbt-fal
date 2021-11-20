from dataclasses import dataclass
from typing import Optional

from dbt.contracts.graph.manifest import Manifest, MaybeNonSource, MaybeParsedSource

import faldbt.lib as lib
from faldbt import parse
from faldbt.project import DbtProject

import pandas as pd


@dataclass
class FalDbt:
    project_dir: str
    profiles_dir: str
    keyword: str  # TODO: not really necessary here?

    _dbt_project: DbtProject
    _manifest: Manifest

    def __init__(self, project_dir: str, profiles_dir: str, _keyword: str = None):
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.keyword = _keyword

        # TODO: do not relay on this to build this object?
        self._dbt_project = parse.parse_project(
            project_dir=project_dir, profiles_dir=profiles_dir, keyword=self.keyword
        )
        self._manifest = self._dbt_project.manifest.nativeManifest

    def get_filtered_models(self, all: bool):
        return self._dbt_project.get_filtered_models(all)

    def ref(
        self, target_model_name: str, target_package_name: Optional[str] = None
    ) -> pd.DataFrame:
        target_model: MaybeNonSource = self._manifest.resolve_ref(
            target_model_name, target_package_name, self.project_dir, self.project_dir
        )

        if target_model is None:
            raise Exception(
                f"Could not find model {target_model_name}.{target_package_name or ''}"
            )

        result = lib.fetch_target(self._manifest, self.project_dir, target_model)
        return pd.DataFrame.from_records(
            result.table.rows, columns=result.table.column_names
        )

    def source(self, target_source_name: str, target_table_name: str) -> pd.DataFrame:
        target_source: MaybeNonSource = self._manifest.resolve_source(
            target_source_name, target_table_name, self.project_dir, self.project_dir
        )

        if target_source is None:
            raise Exception(
                f"Could not find source '{target_source_name}'.'{target_table_name}'"
            )

        result = lib.fetch_target(self._manifest, self.project_dir, target_source)
        return pd.DataFrame.from_records(
            result.table.rows, columns=result.table.column_names
        )

    def write_to_source(
        self, data: pd.DataFrame, target_source_name: str, target_table_name: str
    ):
        target_source: MaybeParsedSource = self._manifest.resolve_source(
            target_source_name, target_table_name, self.project_dir, self.project_dir
        )

        if target_source is None:
            raise Exception(
                f"Could not find source '{target_source_name}'.'{target_table_name}'"
            )

        lib.write_target(data, self._manifest, self.project_dir, target_source)
