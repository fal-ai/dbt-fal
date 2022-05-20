"""Classes and functions for managing features."""
from dataclasses import dataclass


@dataclass
class Feature:
    """Feature is a column in a dbt model."""

    model: str
    column: str
    entity_column: str
    timestamp_column: str
    description: str

    def get_name(self) -> str:
        """Return a generated unique name for this feature."""
        return f"{self.model}.{self.column}"
