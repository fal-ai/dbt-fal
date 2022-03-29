from dataclasses import dataclass, field
from typing import List, TypeVar, Dict, Union
from faldbt.project import DbtModel, FalDbt
from pathlib import Path
import sys

T = TypeVar("T", bound="FalScript")


class FalDagCycle(Exception):
    pass


@dataclass(frozen=True)
class FalScript:
    model: Union[DbtModel, None]
    path: Path

    def exec(self, context, faldbt: FalDbt):
        """
        Executes the script
        """

        # Enable local imports
        local_path = str(self.path.parent)
        try:
            # NOTE: since this happens in threads, the `local_path` available
            # in `sys.path` for all scripts running at the same time.
            # This may introduce undesired race conditions for users.
            # We probably want to pass the `sys.path` of each separately
            sys.path.append(local_path)

            with open(self.path) as file:
                source_code = compile(file.read(), self.path, "exec")

            exec(
                source_code,
                {
                    "context": context,
                    "ref": faldbt.ref,
                    "source": faldbt.source,
                    "write_to_source": faldbt.write_to_source,
                    "write_to_firestore": faldbt.write_to_firestore,
                    "list_models": faldbt.list_models,
                    "list_models_ids": faldbt.list_models_ids,
                    "list_sources": faldbt.list_sources,
                    "list_features": faldbt.list_features,
                    "el": faldbt.el,
                },
            )
        finally:
            sys.path.remove(local_path)
