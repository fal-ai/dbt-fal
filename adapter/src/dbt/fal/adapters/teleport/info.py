from pathlib import Path
from dataclasses import dataclass
from typing import Any, Union

from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.relation import ComponentName

@dataclass
class TeleportInfo:
    format: str
    credentials: Any

    @classmethod
    def relation_name(cls, relation: Union[str, BaseRelation]):
        if isinstance(relation, str):
            # TODO: should we check for quoting?
            return relation
        else:
            path = relation.path
            db = path.get_lowered_part(ComponentName.Database)
            sc = path.get_lowered_part(ComponentName.Schema)
            tb = path.get_lowered_part(ComponentName.Identifier)
            return f"{db}.{sc}.{tb}"

    def build_relation_path(self, relation: Union[str, BaseRelation]):
        rel_name = TeleportInfo.relation_name(relation)
        return rel_name + "." + self.format

    def build_url(self, path: str) -> str:
        raise NotImplemented

@dataclass
class LocalTeleportInfo(TeleportInfo):
    base_dir: Path

    def __init__(self, format: str, credentials: Any, base_dir: Union[str, Path]):
        super().__init__(format, credentials)

        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        assert self.base_dir.is_dir(), "Local Teleport base_dir should be a directory"

    def build_url(self, path: str):
        return self.base_dir / path

# TODO: How to do Teleport Configuration
# Will each adapter have to implement a specific case for each teleport backend.
# And how will we generalize these configurations?
#
# Each adapter will have a specific `teleport` property.
#       my_db:
#         type: duckdb
#         path: ./local_file.db
#         teleport:
#             type: s3
#             bucket: my_bucket
#             access_key_id: ...
#             secret_access_key: ...
#       my_py:
#         type: fal
#         teleport:
#             type: s3
#             s3_bucket: my_bucket
#             s3_access_key_id: ...
#             s3_access_key: ...
# Since all adapters could use different properties and methods to achieve this
@dataclass
class S3TeleportInfo(TeleportInfo):
    bucket: str
    inner_path: str

    def build_url(self, path: str):
        return f's3://{self.bucket}/{self.inner_path}/{path}'
