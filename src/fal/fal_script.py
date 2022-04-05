from dataclasses import dataclass, field
from typing import List, TypeVar, Dict, Union
from faldbt.project import DbtModel, FalDbt
from pathlib import Path

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
        try:

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

    @property
    def id(self):
        id_str = "("
        if self.model:
            id_str += self.model.name
        else:
            id_str += "<GLOBAL>"

        # TODO: maybe `self.path - project_dir`, to show only relevant path
        id_str += "," + str(self.path) + ")"
        return id_str
