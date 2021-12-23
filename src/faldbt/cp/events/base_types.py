# NOTE: COPIED FROM https://github.com/dbt-labs/dbt-core/blob/43edc887f97e359b02b6317a9f91898d3d66652b/core/dbt/events/base_types.py
from abc import ABCMeta, abstractmethod, abstractproperty
from dataclasses import dataclass
from datetime import datetime
import os
import threading
from typing import Any, Optional

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# These base types define the _required structure_ for the concrete event #
# types defined in types.py                                               #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


class Cache:
    # Events with this class will only be logged when the `--log-cache-events` flag is passed
    pass


@dataclass
class Node:
    node_path: str
    node_name: str
    unique_id: str
    resource_type: str
    materialized: str
    node_status: str
    node_started_at: datetime
    node_finished_at: Optional[datetime]
    type: str = "node_status"


@dataclass
class ShowException:
    # N.B.:
    # As long as we stick with the current convention of setting the member vars in the
    # `message` method of subclasses, this is a safe operation.
    # If that ever changes we'll want to reassess.
    def __post_init__(self):
        self.exc_info: Any = True
        self.stack_info: Any = None
        self.extra: Any = None


# TODO add exhaustiveness checking for subclasses
# can't use ABCs with @dataclass because of https://github.com/python/mypy/issues/5374
# top-level superclass for all events
class Event(metaclass=ABCMeta):
    # fields that should be on all events with their default implementations
    log_version: int = 1
    ts: Optional[datetime] = None  # use getter for non-optional
    ts_rfc3339: Optional[str] = None  # use getter for non-optional
    pid: Optional[int] = None  # use getter for non-optional
    node_info: Optional[Node]

    # four digit string code that uniquely identifies this type of event
    # uniqueness and valid characters are enforced by tests
    @abstractproperty
    @staticmethod
    def code() -> str:
        raise Exception("code() not implemented for event")

    # do not define this yourself. inherit it from one of the above level types.
    @abstractmethod
    def level_tag(self) -> str:
        raise Exception("level_tag not implemented for Event")

    # Solely the human readable message. Timestamps and formatting will be added by the logger.
    # Must override yourself
    @abstractmethod
    def message(self) -> str:
        raise Exception("msg not implemented for Event")

    # exactly one time stamp per concrete event
    def get_ts(self) -> datetime:
        if not self.ts:
            self.ts = datetime.utcnow()
            self.ts_rfc3339 = self.ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return self.ts

    # preformatted time stamp
    def get_ts_rfc3339(self) -> str:
        if not self.ts_rfc3339:
            # get_ts() creates the formatted string too so all time logic is centralized
            self.get_ts()
        return self.ts_rfc3339  # type: ignore

    # exactly one pid per concrete event
    def get_pid(self) -> int:
        if not self.pid:
            self.pid = os.getpid()
        return self.pid

    # in theory threads can change so we don't cache them.
    def get_thread_name(self) -> str:
        return threading.current_thread().getName()

    @classmethod
    def get_invocation_id(cls) -> str:
        from dbt.events.functions import get_invocation_id

        return get_invocation_id()

    # default dict factory for all events. can override on concrete classes.
    @classmethod
    def asdict(cls, data: list) -> dict:
        d = dict()
        for k, v in data:
            # stringify all exceptions
            if isinstance(v, Exception) or isinstance(v, BaseException):
                d[k] = str(v)
            # skip all binary data
            elif isinstance(v, bytes):
                continue
            else:
                d[k] = v
        return d


# in preparation for #3977
class TestLevel(Event):
    def level_tag(self) -> str:
        return "test"


class DebugLevel(Event):
    def level_tag(self) -> str:
        return "debug"


class InfoLevel(Event):
    def level_tag(self) -> str:
        return "info"


class WarnLevel(Event):
    def level_tag(self) -> str:
        return "warn"


class ErrorLevel(Event):
    def level_tag(self) -> str:
        return "error"


@dataclass  # type: ignore
class NodeInfo(Event, metaclass=ABCMeta):
    report_node_data: Any  # Union[ParsedModelNode, ...] TODO: resolve circular imports

    def get_node_info(self):
        node_info = Node(
            node_path=self.report_node_data.path,
            node_name=self.report_node_data.name,
            unique_id=self.report_node_data.unique_id,
            resource_type=self.report_node_data.resource_type.value,
            materialized=self.report_node_data.config.get("materialized"),
            node_status=str(self.report_node_data._event_status.get("node_status")),
            node_started_at=self.report_node_data._event_status.get("started_at"),
            node_finished_at=self.report_node_data._event_status.get("finished_at"),
        )
        return node_info


# prevents an event from going to the file
class NoFile:
    pass


# prevents an event from going to stdout
class NoStdOut:
    pass
