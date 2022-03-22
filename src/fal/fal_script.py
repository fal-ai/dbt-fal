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
        sys.path.append(local_path)

        with open(self.path) as file:
            a_script = file.read()
            exec(
                a_script,
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
            sys.path.remove(local_path)
