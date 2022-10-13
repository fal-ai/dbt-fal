import abc
from typing import List, Union

from dbt.exceptions import NotImplementedException
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.base.impl import BaseAdapter

from dbt.fal.adapters.teleport.info import TeleportInfo

class TeleportAdapter:
    """The TeleportAdapter provides an abstract class for adapters usable with Teleport.

    Adapters must implement the following methods and macros. Some of the
    methods can be safely overridden as a noop, where it makes sense. Those
    methods are marked with a (passable) in their docstrings. Check docstrings
    for type information, etc.

    To implement a macro, implement "${adapter_type}__${macro_name}" in the
    adapter's internal project.

    To invoke a method in an adapter macro, call it on the 'adapter' Jinja
    object using dot syntax.

    To invoke a method in model code, add the @available decorator atop a method
    declaration. Methods are invoked as macros.

    Methods:
        - storage_formats
        # - teleport_backends
        - teleport_from_external_storage
        - teleport_to_external_storage

    Macros:
        -

    """

    @classmethod
    @abc.abstractmethod
    def storage_formats(cls) -> List[str]:
        """
        List of formats this adapter handles. e.g. `['csv', 'parquet']`
        """
        raise NotImplementedException(
            "`storage_formats` is not implemented for this adapter!"
        )

    @abc.abstractmethod
    def teleport_from_external_storage(
        self, relation: BaseRelation, data_path: str, teleport_info: TeleportInfo
    ) -> None:
        """
        Localize data found in `data_path` into the a local table with the name given by `relation`.
        Handle each possible format defined in `storage_formats` method.
        """
        raise NotImplementedException(
            "`teleport_from_external_storage` is not implemented for this adapter!"
        )

    @abc.abstractmethod
    def teleport_to_external_storage(self, relation: BaseRelation, teleport_info: TeleportInfo) -> str:
        """
        Take local table `relation` and upload the data with Teleport. Return the `data_path` the data is uploaded to.
        Handle each possible format defined in `storage_formats` method.
        """
        raise NotImplementedException(
            "`teleport_to_external_storage` is not implemented for this adapter!"
        )

    @classmethod
    def is_teleport_adapter(cls, adapter: Union[BaseAdapter, "TeleportAdapter"]) -> bool:
        methods = [
            "storage_formats",
            # "teleport_backends",
            "teleport_from_external_storage",
            "teleport_to_external_storage",
        ]
        return isinstance(adapter, TeleportAdapter) or all(map(lambda m: hasattr(adapter, m), methods))

    @classmethod
    def find_format(cls, target_adapter: "TeleportAdapter", ref_adapter: "TeleportAdapter"):
        """
        Find common format between target and ref adapter, giving priority to target list ordering.
        """
        target_formats = target_adapter.storage_formats()
        ref_formats = ref_adapter.storage_formats()
        for format in target_formats:
            if format in ref_formats:
                return format

        raise RuntimeError(
            f"No common format between {target_adapter.type()} and {ref_adapter.type()} "
            f"—  {target_formats} | {ref_formats}"
        )
