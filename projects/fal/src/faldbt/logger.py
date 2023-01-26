from dataclasses import dataclass

from dbt import ui
from dbt.logger import log_manager  # For export
from dbt.events.functions import fire_event

import faldbt.version as version

from dbt.events.types import (
    AdapterEventDebug as _DebugLevel,
    AdapterEventInfo as _InfoLevel,
    AdapterEventWarning as _WarnLevel,
    AdapterEventError as _ErrorLevel,
)

class FireEventLogger:
    def debug(self, msg: str, *args, **kwargs):
        fire_event(_version_handler(AdapterEventDebug, _prepare_msg(msg, *args, **kwargs)))

    def info(self, msg: str, *args, **kwargs):
        fire_event(_version_handler(AdapterEventInfo, _prepare_msg(msg, *args, **kwargs)))

    def warn(self, msg: str, *args, **kwargs):
        fire_event(_version_handler(AdapterEventWarning, _prepare_msg(msg, *args, **kwargs)))

    def warning(self, msg: str, *args, **kwargs):
        # Alias to warn
        return self.warn(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        fire_event(_version_handler(AdapterEventError, _prepare_msg(msg, *args, **kwargs)))


def _prepare_msg(msg: str, *args, **kwargs):
    if args or kwargs:
        return msg.format(*args, **kwargs)
    else:
        return msg

def _version_handler(cls, msg):
    if version.is_version_plus('1.4.0'):
        from dbt.events.proto_types import NodeInfo

        return cls(
            node_info=NodeInfo("","","","","","","","",{}),
            name="fal-cli",
            args=[],
            base_msg=msg
        )
    else:
        return cls(
            name="fal-cli",
            args=[],
            base_msg=msg
        )


LOGGER = FireEventLogger()

if version.is_version_plus("1.1.0"):
    _EXTRA_CLASS_INHERIT = ()
else:
    from dbt.events.base_types import Cli, Event

    _EXTRA_CLASS_INHERIT = Cli, Event


@dataclass
class AdapterEventDebug(_DebugLevel, *_EXTRA_CLASS_INHERIT):
    def code(self):
        return "FAL1000"

    def message(self) -> str:
        return self.base_msg


@dataclass
class AdapterEventInfo(_InfoLevel, *_EXTRA_CLASS_INHERIT):
    def code(self):
        return "FAL2000"

    def message(self) -> str:
        return self.base_msg


@dataclass
class AdapterEventWarning(_WarnLevel, *_EXTRA_CLASS_INHERIT):
    def code(self):
        return "FAL3000"

    def message(self) -> str:
        return ui.warning_tag(self.base_msg)


@dataclass
class AdapterEventError(_ErrorLevel, *_EXTRA_CLASS_INHERIT):
    def code(self):
        return "FAL4000"

    def message(self) -> str:
        return self.base_msg
