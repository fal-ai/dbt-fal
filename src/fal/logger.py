from dataclasses import dataclass

from dbt.logger import log_manager
from dbt.events.functions import fire_event
from dbt.events.base_types import (
    DebugLevel as _DebugLevel,
    InfoLevel as _InfoLevel,
    WarnLevel as _WarnLevel,
    ErrorLevel as _ErrorLevel,
)

# TODO: do we still need to do this?
def reconfigure_logging() -> None:
    # Disabling the dbt.logger.DelayedFileHandler manually
    # since we do not use the new dbt logging system
    # This fixes issue https://github.com/fal-ai/fal/issues/97
    log_manager.set_path(None)

@dataclass
class DebugMessage(_DebugLevel):
    msg: str
    code: str = "FAL1000"

    def message(self) -> str:
        return self.msg


@dataclass
class InfoMessage(_InfoLevel):
    msg: str
    code: str = "FAL2000"

    def message(self) -> str:
        return self.msg


@dataclass
class WarnMessage(_WarnLevel):
    msg: str
    code: str = "FAL3000"

    def message(self) -> str:
        return self.msg


@dataclass
class ErrorMessage(_ErrorLevel):
    msg: str
    code: str = "FAL4000"

    def message(self) -> str:
        return self.msg

