# A Python native two-way bridge implementation between the controller
# (fal-cli) process and the child process(es) (isolated scripts).
#
#    controller       both       children
# /--------------\ /--------\ /------------\
# object -> dill -> socket -> dill -> object
#
#
# The controller starts the listener (server) and then passes the raw
# socket address to the children. Each child then connects to the server
# and from that point onward, all exchanges (via send/recv) are done via
# regular Python objects (using dill).
#


from __future__ import annotations

from contextlib import closing
from functools import partial
from multiprocessing.connection import Client, ConnectionWrapper, Listener

import base64
import dill

dill_wrapper = partial(ConnectionWrapper, dumps=dill.dumps, loads=dill.loads)


class _DillListener(Listener):
    def accept(self) -> ConnectionWrapper:
        return closing(dill_wrapper(super().accept()))


def controller_connection() -> _DillListener:
    return _DillListener()


def child_connection(address: bytes) -> ConnectionWrapper:
    return closing(dill_wrapper(Client(address)))


def encode_service_address(address: bytes) -> str:
    return base64.b64encode(address).decode("utf-8")


def decode_service_address(address: str) -> bytes:
    return base64.b64decode(address)
