"""Classes and functions for managing features."""
from dataclasses import dataclass
from typing import List
# from faldbt.project import FalDbt
from faldbt.lib import _get_target_relation
from faldbt.parse import get_dbt_config
from google.cloud import bigquery


@dataclass
class Feature:
    """Feature is a column in a dbt model."""

    model: str
    column: str
    entity_id: str
    timestamp: str
    description: str

    def get_name(self) -> str:
        """Return a generated unique name for this feature."""
        return f"{self.model}.{self.column}"


@dataclass
class Dataset:
    """Dataset is a labeled set of features."""

    name: str
    features: List[Feature]
    label: Feature


def create_dataset(name: str,
                   features: List[Feature],
                   label: Feature,
                   falDbt) -> Dataset:
    """Join features with a label and stores result in a new table."""
    query = _build_stage_join_query(
        model_name=features[0].model,
        features=features,
        label=label,
        falDbt=falDbt
    )

    # TODO: Handle credentials
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        destination="learning-project-305919.dbt_meder_bike.bike_label")
    job = client.query(query=query, job_config=job_config)
    job.result()

    return Dataset(name=name, features=features, label=label)


def _build_stage_join_query(model_name: str,
                            features: List[Feature],
                            label: Feature,
                            falDbt):

    feature_table = _get_table(falDbt, model_name)
    label_table = _get_table(falDbt, label.model)
    feature_strs = []
    prefixed_feature_strs = []
    features_entity_id = features[0].entity_id
    features_timestamp = features[0].timestamp
    for feature in features:
        feature_strs.append(feature.column)
        prefixed_feature_strs.append(f"f.{feature.column}")
    features_selector = ', '.join(feature_strs)

    features_query = f'''
    SELECT {features_selector},
      {features_entity_id},
      lead({features_timestamp}) over __f__timestamp_window as __f__next_time,
      {features_timestamp} as __f__available_time
    FROM {feature_table}
    WINDOW __f__timestamp_window AS (
      PARTITION BY {features_entity_id}
      ORDER BY {features_timestamp} ASC
    )
    '''

    label_query = f'''
    SELECT {label.column},
      {label.entity_id},
      {label.timestamp},
      CAST({label.timestamp} AS TIMESTAMP) as _timestamp,
    FROM {label_table}
    '''

    return f'''
    WITH features AS (
      {features_query}
    ),
    label AS (
      {label_query}
    )
    SELECT
      l.{label.entity_id},
      {label.timestamp},
      {', '.join(prefixed_feature_strs)},
      l.{label.column},
      CAST(f.__f__available_time AS DATE) as feature_date
    FROM label AS l
    LEFT JOIN features AS f
      ON CAST(l.{label.entity_id} AS STRING) = CAST(f.{features_entity_id} AS STRING)
      AND f.__f__available_time < l._timestamp
      AND (f.__f__next_time IS NULL OR l._timestamp <= f.__f__next_time)
    '''


def _get_table(falDbt, model_name: str):
    """Get warehouse  table reference for a given feature."""
    feature_target = falDbt._manifest.nativeManifest.resolve_ref(
        model_name, None, falDbt.project_dir, falDbt.project_dir
    )

    if feature_target is None:
        raise Exception(
            f"Could not find model {model_name}."
        )
    feature_table = _get_target_relation(
        feature_target, falDbt.project_dir, falDbt.profiles_dir)

    return feature_table
