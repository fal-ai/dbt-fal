from __future__ import annotations

from contextlib import suppress

import fal_serverless
import pytest
from fal_serverless.api import FalServerlessError

PACKAGE_NAME = "fal_serverless"


def test_missing_dependencies_nested_server_error(isolated_client):
    @isolated_client()
    def test1():
        return "hello"

    @isolated_client()
    def test2():
        test1()

    with pytest.raises(
        FalServerlessError,
        match=r"accessed through 'test1'",
    ):
        test2()


def test_regular_function(isolated_client):
    @isolated_client("virtualenv")
    def regular_function():
        return 42

    assert regular_function() == 42

    @isolated_client("virtualenv")
    def mult(a, b):
        return a * b

    assert mult(5, 2) == 10


def test_function_pipelining(isolated_client):
    @isolated_client("virtualenv")
    def regular_function():
        return 42

    @isolated_client("virtualenv")
    def calling_function(result):
        return result * 2

    assert calling_function(regular_function()) == 84


# TODO: remove the mark
@pytest.mark.xfail(reason="It will fix itself after the new serverless is released")
def test_function_calling_other_function(isolated_client):
    try:
        import importlib.metadata as importlib_metadata
    except ImportError:
        import importlib_metadata

    fal_serverless_version = importlib_metadata.version(PACKAGE_NAME)

    @isolated_client("virtualenv")
    def regular_function():
        return 42

    @isolated_client(
        "virtualenv",
        requirements=[
            f"{PACKAGE_NAME}>=0.6.19,<={fal_serverless_version}",
        ],
    )
    def calling_function(recurse):
        import os

        for name in os.environ:
            if name.startswith("FAL_"):
                print(os.environ[name])

        if recurse:
            return calling_function(recurse=False)
        else:
            return regular_function()

    # One level of direct calls
    assert calling_function(recurse=False) == 42

    # Two levels of direct calls
    assert calling_function(recurse=True) == 42


@pytest.mark.xfail
def test_dependency_inference(isolated_client):
    @isolated_client("virtualenv")
    def regular_function():
        return 42

    @isolated_client("virtualenv")
    def calling_function():
        return regular_function()

    assert calling_function() == 42


@pytest.mark.xfail
def test_process_crash(isolated_client):
    # We can catch the SystemExit if it originates from
    # an exception (in the case of exit(1)).
    @isolated_client("virtualenv")
    def process_crash_regular():
        exit(1)

    with pytest.raises(SystemExit):
        process_crash_regular()

    # But if it is a native exit call, then the agent process
    # will crash and there is not much we can do other than offer
    # a decent error message.
    @isolated_client("virtualenv")
    def process_crash_without_catching():
        import os

        os._exit(0)

    with pytest.raises(FalServerlessError, match="..."):
        process_crash_without_catching()


@pytest.mark.xfail
def test_unserializable_input_function(isolated_client):
    # When the function can not be serialized at all (has a reference to an unpicklable
    # object (like a frame)).

    @isolated_client("virtualenv")
    def unpicklable_input_function_client(default=__import__("sys")._getframe(0)):
        return default

    with pytest.raises(FalServerlessError, match="..."):
        unpicklable_input_function_client()


@pytest.mark.xfail
def test_unserializable_return(isolated_client):
    # When the return of the function can not be serialized.

    @isolated_client("virtualenv")
    def unpicklable_return():
        import sys

        return sys._getframe(0)

    with pytest.raises(FalServerlessError, match="..."):
        unpicklable_return()


@pytest.mark.xfail
def test_missing_dependencies_on_client(isolated_client):
    # When the return can't be deserialized on the client side due to
    # incomplete/missing dependencies.

    @isolated_client("virtualenv", requirements=["refactor"])
    def unpicklable_input_function_client():
        import refactor

        return refactor.BaseAction()

    with pytest.raises(FalServerlessError, match="..."):
        unpicklable_input_function_client()


@pytest.mark.xfail
def test_when_exception_can_not_be_deserialized(isolated_client):
    # When the exception can't be deserialized on the client side due to
    # incomplete/missing dependencies.

    @isolated_client("virtualenv", requirements=[""])
    def unpicklable_input_function_client():
        class T(Exception):
            frame = __import__("sys")._getframe(0)

        raise T()

    with pytest.raises(FalServerlessError, match="..."):
        unpicklable_input_function_client()


@pytest.mark.xfail
def test_client_superseeding_dependencies_crash(isolated_client):
    # E.g. I depend on tensorboard which depends on dill==0.3.2
    # but the agent overrides it with dill==0.3.5.1 so I get an
    # unexpected error at runtime (instead of at environment creation
    # time)

    @isolated_client("virtualenv", requirements=["dill==0.3.2"])
    def conflicting_environment():
        import dill

        assert dill.__version__ == "0.3.2"

    # This should crash at the build stage
    with pytest.raises(
        FalServerlessError, match="package versions have conflicting dependencies."
    ):
        conflicting_environment()


@pytest.mark.xfail
def test_memory_overflow_crash_on_run(isolated_client):
    @isolated_client("virtualenv")
    def memory_overflow_crash_on_run():
        import os

        objects = []
        while True:
            objects.append(os.urandom(1024**3))

    with pytest.raises(Exception, match="Insufficient memory"):
        memory_overflow_crash_on_run()


def test_keepalive_after_agent_exit(isolated_client):
    # Should work (fresh)
    @isolated_client("virtualenv")
    def regular_function():
        return 42

    assert regular_function() == 42

    # This uses the cached agent process but it will
    # kill it at the end.
    with suppress(BaseException):

        @isolated_client("virtualenv")
        def process_crash_without_catching():
            import os

            os._exit(0)

        process_crash_without_catching()

    # This should start a fresh agent process.
    @isolated_client("virtualenv")
    def regular_function():
        return 42

    assert regular_function() == 42


def test_faulty_setup_function(isolated_client):
    def good_setup_function():
        return 42

    def bad_setup_function():
        raise ValueError()

    @isolated_client("virtualenv")
    def regular_function(result):
        return result * 2

    first_case = regular_function.on(setup_function=good_setup_function)
    assert first_case() == 84

    second_case = regular_function.on(setup_function=bad_setup_function)
    with pytest.raises(ValueError):
        second_case()


def test_unserializable_setup_function(isolated_client):
    def unpicklable_setup_function():
        import sys

        return sys._getframe(0)

    @isolated_client("virtualenv", setup_function=unpicklable_setup_function)
    def regular_function(result):
        return str(result)

    # This works fine since the setup function's result is not returned
    # to the client (but rather just kept on the server).
    assert isinstance(regular_function(), str)


def test_big_message(isolated_client):
    # Default gRPC max message size is 4MB so let's
    # try doubling that.
    data_length = 8 * (1024**2)

    @isolated_client("virtualenv", machine_type="M")
    def big_input_function(data):
        return len(data)

    # Small inputs should work fine (pre-test).
    assert big_input_function(b"0") == 1
    assert big_input_function(b"0" * data_length) == data_length

    # Try receiving a big message.
    @isolated_client("virtualenv", machine_type="M")
    def big_return_function(data_length):
        return b"0" * data_length

    assert len(big_return_function(1)) == 1
    assert len(big_return_function(data_length)) == data_length


def test_futures(isolated_client):
    from concurrent.futures import wait

    @isolated_client("virtualenv")
    def regular_function(n):
        return n * 2

    future = regular_function.submit(1)
    assert future.result() == 2

    future_2 = regular_function.submit(2)
    future_3 = regular_function.submit(3)
    future_4 = regular_function.submit(4)
    wait([future_2, future_3, future_4])

    # Ensure the futures are done in the correct order
    # (no mix and match between arguments and the returned
    # future).
    assert future_2.result() == 4
    assert future_3.result() == 6
    assert future_4.result() == 8


def test_conda_environment(isolated_client):
    @isolated_client(
        "conda", packages=["pyjokes=0.6.0"], machine_type="M", resolver="conda"
    )
    def regular_function():
        import pyjokes

        return pyjokes.__version__

    assert regular_function() == "0.6.0"


def test_cached_function(isolated_client, capsys, monkeypatch):
    import inspect
    import time

    test_stamp = time.time()
    real_getsource = inspect.getsource

    # For ensuring between test runs the cached functions
    # are gone, we add a timestamp to the source code.
    def add_timestamp_to_source(func):
        return real_getsource(func) + f"\n# {test_stamp}"

    monkeypatch.setattr(inspect, "getsource", add_timestamp_to_source)

    @fal_serverless.cached
    def get_pipe():
        import time

        print("computing")
        time.sleep(1)  # slow IO
        return "pipe"

    @fal_serverless.cached
    def factorial(n: int) -> int:
        import math
        import time

        print("computing")
        time.sleep(1)  # slow CPU
        return math.factorial(n)

    @isolated_client("virtualenv", keep_alive=30)
    def regular_function(n):
        if get_pipe() == "pipe":
            return factorial(n)

    assert regular_function(4) == 24
    out, err = capsys.readouterr()
    assert out.count("computing") == 2

    # pipe is now cached, but using a different factorial(n)
    assert regular_function(3) == 6
    out, err = capsys.readouterr()
    assert out.count("computing") == 1

    # This should be all cached
    assert regular_function(4) == 24
    out, err = capsys.readouterr()
    assert out.count("computing") == 0

    # So does thiss
    assert regular_function(3) == 6
    out, err = capsys.readouterr()
    assert out.count("computing") == 0

    # But this should not be cached
    assert regular_function(5) == 120
    out, err = capsys.readouterr()
    assert out.count("computing") == 1
