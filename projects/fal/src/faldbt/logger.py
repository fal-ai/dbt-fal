from __future__ import annotations

from dataclasses import dataclass

import faldbt.version as version
from dbt import ui
from dbt.events.base_types import DebugLevel as _DebugLevel
from dbt.events.base_types import ErrorLevel as _ErrorLevel
from dbt.events.base_types import InfoLevel as _InfoLevel
from dbt.events.base_types import TestLevel as _TestLevel
from dbt.events.base_types import WarnLevel as _WarnLevel
from dbt.events.functions import fire_event


class FireEventLogger:
    def test(self, msg: str, *args, **kwargs):
        fire_event(TestMessage(_prepare_msg(msg, *args, **kwargs)))

    def trace(self, msg: str, *args, **kwargs):
        # Alias to test
        return self.test(msg, *args, **kwargs)

    def debug(self, msg: str, *args, **kwargs):
        fire_event(DebugMessage(_prepare_msg(msg, *args, **kwargs)))

    def info(self, msg: str, *args, **kwargs):
        fire_event(InfoMessage(_prepare_msg(msg, *args, **kwargs)))

    def warn(self, msg: str, *args, **kwargs):
        fire_event(WarnMessage(_prepare_msg(msg, *args, **kwargs)))

    def warning(self, msg: str, *args, **kwargs):
        # Alias to warn
        return self.warn(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        fire_event(ErrorMessage(_prepare_msg(msg, *args, **kwargs)))


def _prepare_msg(msg: str, *args, **kwargs):
    if args or kwargs:
        return msg.format(*args, **kwargs)
    else:
        return msg


LOGGER = FireEventLogger()

if version.version_compare("1.1.0") < 0:
    from dbt.events.base_types import Cli, Event

    _EXTRA_CLASS_INHERIT = Cli, Event
else:
    _EXTRA_CLASS_INHERIT = (object,)


@dataclass
class TestMessage(_TestLevel, *_EXTRA_CLASS_INHERIT):
    msg: str
    code: str = "FAL0000"

    def message(self) -> str:
        return self.msg


@dataclass
class DebugMessage(_DebugLevel, *_EXTRA_CLASS_INHERIT):
    msg: str
    code: str = "FAL1000"

    def message(self) -> str:
        return self.msg


@dataclass
class InfoMessage(_InfoLevel, *_EXTRA_CLASS_INHERIT):
    msg: str
    code: str = "FAL2000"

    def message(self) -> str:
        return self.msg


@dataclass
class WarnMessage(_WarnLevel, *_EXTRA_CLASS_INHERIT):
    msg: str
    code: str = "FAL3000"

    def message(self) -> str:
        return ui.warning_tag(self.msg)


@dataclass
class ErrorMessage(_ErrorLevel, *_EXTRA_CLASS_INHERIT):
    msg: str
    code: str = "FAL4000"

    def message(self) -> str:
        return self.msg
