from __future__ import annotations

import os

from pandas import DataFrame

table_prefix = f"ns__{ os.environ.get('DB_NAMESPACE', '') }__ns__"

write_to_source(DataFrame({"a": [1, 2, 3]}), "results", table_prefix + "some_source")
