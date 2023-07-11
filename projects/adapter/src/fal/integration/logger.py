from contextlib import contextmanager
import logging
import datetime
import sys

import dbt.ui as ui
from dbt.logger import log_manager as dbt_log_manager

TRACE = logging.DEBUG - 5
logging.addLevelName(TRACE, "TRACE")

class FalLogger:
    def __init__(self):
        self._stdout_handler = logging.StreamHandler(sys.stdout)
        self._stdout_handler.setLevel(logging.INFO)

        self._logger = logging.Logger("fal", logging.INFO)
        self._logger.addHandler(self._stdout_handler)

    def __getstate__(self):
        # Don't pickle the logger
        d = self.__dict__.copy()
        d['_logger'] = d['_logger'].name
        return d

    def __setstate__(self, d):
        # Set logger when unpickling
        d['_logger'] = logging.Logger(
            d['_logger'] if isinstance(d['_logger'], str) else "fal", logging.INFO)
        self.__dict__.update(d)

    def set_level(self, level: int):
        self._logger.setLevel(level)

    @property
    def level(self):
        return self._logger.level

    def log(self, level: int, msg: str, *args, **kwargs):
        now = datetime.datetime.now(datetime.timezone.utc)
        if self.level <= logging.DEBUG:
            prefix = now.strftime(r"%H:%M:%S.%f")

            prefix += f" [{logging.getLevelName(level).lower()[0:5].ljust(5)}]"

            # Spaces to match the spacing of dbt's debug logs, like the following
            #
            #     21:32:31.816189 [debug] [MainThread]: Flushing usage events
            #     21:32:32.530385 [error] [fal       ]: Error in script (...):
            #     21:32:34.554038 [debug] [Thread-1  ]: Opening a new connection, currently in state closed
            prefix += " [fal       ]:"
        else:
            prefix = now.strftime("%H:%M:%S")
            prefix += " [fal]:"

        self._logger.log(level, f"{prefix} {_prepare_msg(msg, *args, **kwargs)}")

    def trace(self, msg: str, *args, **kwargs):
        self.log(TRACE, msg, *args, **kwargs)

    def debug(self, msg: str, *args, **kwargs):
        self.log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs):
        self.log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs):
        self.log(logging.WARNING, ui.warning_tag(msg), *args, **kwargs)

    def warn(self, msg: str, *args, **kwargs):
        # Alias to warning
        return self.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs):
        self.log(logging.ERROR, msg, *args, **kwargs)

class LogManager():
    def __init__(self, dbt_log_manager):
        self._dbt_log_manager = dbt_log_manager

    @contextmanager
    def applicationbound(self):
        # TODO: probably where we can add threadding information if we want it for debug logs
        with self._dbt_log_manager.applicationbound():
            yield

    def set_debug(self):
        self._dbt_log_manager.set_debug()
        LOGGER.set_level(logging.DEBUG)

    def set_trace(self):
        self._dbt_log_manager.set_debug()
        LOGGER.set_level(TRACE)

def _prepare_msg(msg: str, *args, **kwargs):
    if args or kwargs:
        return msg.format(*args, **kwargs)
    else:
        return msg

LOGGER = FalLogger()
log_manager = LogManager(dbt_log_manager)
