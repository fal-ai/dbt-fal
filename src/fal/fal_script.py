from dataclasses import dataclass, field
from typing import List, TypeVar, Dict, Union
from faldbt.project import DbtModel, FalDbt
from pathlib import Path
from functools import partial

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

            exec_globals = {
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
            }

            if self.model is not None:
                # Hard-wire the model
                exec_globals["write_to_model"] = partial(
                    faldbt.write_to_model,
                    target_model_name=self.model.name,
                    target_package_name=None,
                )

            exec(source_code, exec_globals)
        finally:
            pass

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
