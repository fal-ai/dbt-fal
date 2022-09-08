from dataclasses import dataclass

from dbt.logger import log_manager
from dbt.events.functions import fire_event
from dbt.events.base_types import (
    TestLevel as _TestLevel,
    DebugLevel as _DebugLevel,
    InfoLevel as _InfoLevel,
    WarnLevel as _WarnLevel,
    ErrorLevel as _ErrorLevel,
)


class LOGGER:
    @classmethod
    def test(cls, msg: str, *args, **kwargs):
        fire_event(TestMessage(_prepare_msg(msg, *args, **kwargs)))

    @classmethod
    def trace(cls, msg: str, *args, **kwargs):
        # Alias to test
        return cls.test(msg, *args, **kwargs)

    @classmethod
    def debug(cls, msg: str, *args, **kwargs):
        fire_event(DebugMessage(_prepare_msg(msg, *args, **kwargs)))

    @classmethod
    def info(cls, msg: str, *args, **kwargs):
        fire_event(InfoMessage(_prepare_msg(msg, *args, **kwargs)))

    @classmethod
    def warn(cls, msg: str, *args, **kwargs):
        fire_event(WarnMessage(_prepare_msg(msg, *args, **kwargs)))

    @classmethod
    def warning(cls, msg: str, *args, **kwargs):
        # Alias to warn
        return cls.warn(msg, *args, **kwargs)

    @classmethod
    def error(cls, msg: str, *args, **kwargs):
        fire_event(ErrorMessage(_prepare_msg(msg, *args, **kwargs)))


def _prepare_msg(msg: str, *args, **kwargs):
    if args:
        if kwargs:
            return msg.format(*args, **kwargs)
        else:
            return msg.format(*args)
    else:
        if kwargs:
            return msg.format(**kwargs)
        else:
            return msg


# TODO: do we still need to do this?
def reconfigure_logging() -> None:
    # Disabling the dbt.logger.DelayedFileHandler manually
    # since we do not use the new dbt logging system
    # This fixes issue https://github.com/fal-ai/fal/issues/97
    log_manager.set_path(None)


@dataclass
class TestMessage(_TestLevel):
    msg: str
    code: str = "FAL0000"

    def message(self) -> str:
        return self.msg


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
