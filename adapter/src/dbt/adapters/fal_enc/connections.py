from dataclasses import dataclass
from dbt.adapters.fal.connections import FalCredentials

@dataclass
class FalEncCredentials(FalCredentials):

    db_profile: str = ''

    def _connection_keys(self):
        return () + super()._connection_keys()

    @property
    def type(self):
        return "fal_enc"

    @property
    def unique_field(self):
        return self.db_profile
