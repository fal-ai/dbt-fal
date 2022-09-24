from typing import Tuple

from dbt.adapters.base import BaseConnectionManager, Credentials

from dbt.adapters.fal._utils import ignore_implementations


class FalConnectionManager(
    ignore_implementations(
        BaseConnectionManager,
        methods=[
            # These methods are not implemented in the Fal connection
            # manager.
            "exception_handler",
            "cancel_open",
            "open",
            "begin",
            "commit",
            "clear_transaction",
            "execute",
        ],
    )
):
    TYPE = "fal"


# We are going to assume that the dbt itself will create this object
# with the database/schema properties of the encapsulating database.
class FalCredentials(Credentials):
    db_credentials: Credentials

    def type(self) -> str:
        return "fal"

    def _connection_keys(self) -> Tuple[str, ...]:
        return self.db_credentials._connection_keys()
