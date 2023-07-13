import pathlib
import sys
from unittest.mock import Mock, call, patch, ANY
from pathlib import Path

import pytest
import yaml
import datetime
from fal.dbt.telemetry import telemetry


@pytest.fixture
def ignore_fal_stats_enabled_env_var(monkeypatch):
    """
    GitHub Actions configuration scripts set the FAL_STATS_ENABLED
    environment variable to prevent CI events from going to posthog, this
    inferes with some tests. This fixture removes its value temporarily
    """
    monkeypatch.delenv("FAL_STATS_ENABLED", raising=True)


@pytest.fixture
def ignore_env_var_and_set_tmp_default_home_dir(
    tmp_directory, ignore_fal_stats_enabled_env_var, monkeypatch
):
    """
    ignore_fal_stats_enabled_env_var + overrides DEFAULT_HOME_DIR
    to prevent the local configuration to interfere with tests
    """
    monkeypatch.setattr(telemetry, "DEFAULT_HOME_DIR", ".")


# Validations tests
def test_str_validation():
    res = telemetry.str_param("Test")
    assert isinstance(res, str)
    res = telemetry.str_param("TEST")
    assert "TEST" == res
    with pytest.raises(TypeError) as exc_info:
        telemetry.str_param(3)

    exception_raised = exc_info.value
    assert type(exception_raised) == TypeError


def test_opt_str_validation():
    res = telemetry.opt_str_param("")
    assert isinstance(res, str)
    res = telemetry.opt_str_param("TEST")
    assert "TEST" == res
    res = telemetry.opt_str_param(None)
    assert not res

    with pytest.raises(TypeError) as exc_info:
        telemetry.opt_str_param(3)

    exception_raised = exc_info.value
    assert type(exception_raised) == TypeError


def test_check_stats_enabled(ignore_env_var_and_set_tmp_default_home_dir):
    stats_enabled = telemetry.check_stats_enabled()
    assert stats_enabled is True


@pytest.mark.parametrize(
    "yaml_value, expected_first, env_value, expected_second",
    [
        ["true", True, "false", False],
        ["TRUE", True, "FALSE", False],
        ["false", False, "true", True],
        ["FALSE", False, "TRUE", True],
    ],
)
def test_env_var_takes_precedence(
    monkeypatch,
    ignore_env_var_and_set_tmp_default_home_dir,
    yaml_value,
    expected_first,
    env_value,
    expected_second,
):

    stats = Path("stats")
    stats.mkdir()

    (stats / "config.yaml").write_text(f"stats_enabled: {yaml_value}")

    assert telemetry.check_stats_enabled() is expected_first

    monkeypatch.setenv("FAL_STATS_ENABLED", env_value, prepend=False)

    assert telemetry.check_stats_enabled() is expected_second


def test_first_usage(monkeypatch, tmp_directory):
    monkeypatch.setattr(telemetry, "DEFAULT_HOME_DIR", ".")

    stats = Path("stats")
    stats.mkdir()

    # This isn't a first time usage since the config file doesn't exist yet.
    assert not telemetry.check_first_time_usage()
    (stats / "config.yaml").write_text("stats_enabled: True")

    assert telemetry.check_first_time_usage()


# Ref: https://stackoverflow.com/questions/43878953/how-does-one-detect-if-
# one-is-running-within-a-docker-container-within-python
def test_docker_env(monkeypatch):
    def mock(input_path):
        return "dockerenv" in str(input_path)

    monkeypatch.setattr(pathlib.Path, "exists", mock)
    docker = telemetry.is_docker()
    assert docker is True


# Ref https://airflow.apache.org/docs/apache-airflow/stable/
# cli-and-env-variables-ref.html?highlight=airflow_home#envvar-AIRFLOW_HOME
@pytest.mark.parametrize("env_variable", ["AIRFLOW_CONFIG", "AIRFLOW_HOME"])
def test_airflow_env(monkeypatch, env_variable):
    monkeypatch.setenv(env_variable, True)
    airflow = telemetry.is_airflow()
    assert airflow is True


# Ref https://stackoverflow.com/questions/110362/how-can-i-find-
# the-current-os-in-python
@pytest.mark.parametrize("os_param", ["Windows", "Linux", "MacOS", "Ubuntu"])
def test_os_type(monkeypatch, os_param):
    mock = Mock()
    mock.return_value = os_param
    monkeypatch.setattr(telemetry.platform, "system", mock)
    os_type = telemetry.get_os()
    assert os_type == os_param


def test_uid_file():
    uid, uid_error, is_install = telemetry.check_uid()
    assert isinstance(uid, str)


def test_basedir_creation():
    base_dir = telemetry.check_dir_exist()
    assert base_dir.exists()


def test_python_version():
    version = telemetry.python_version()
    assert isinstance(version, str)


def test_stats_off(monkeypatch):
    mock = Mock()
    posthog_mock = Mock()
    mock.patch(telemetry, "check_stats_enabled", False)
    telemetry.log_api("test_action")

    assert posthog_mock.call_count == 0


def test_offline_stats(monkeypatch):
    mock = Mock()
    posthog_mock = Mock()
    mock.patch(telemetry, "is_online", False)
    telemetry.log_api("test_action")

    assert posthog_mock.call_count == 0


def test_is_online():
    assert telemetry.is_online()


def test_is_online_timeout():
    # Check the total run time is less than 1.5 secs
    start_time = datetime.datetime.now()
    telemetry.is_online()
    end_time = datetime.datetime.now()
    total_runtime = end_time - start_time
    assert total_runtime < datetime.timedelta(milliseconds=1500)


def test_is_not_online(monkeypatch):
    mock_httplib = Mock()
    mock_httplib.HTTPSConnection().request.side_effect = Exception
    monkeypatch.setattr(telemetry, "httplib", mock_httplib)
    assert not telemetry.is_online()


def write_to_conf_file(tmp_directory, monkeypatch, last_check):
    stats = Path("stats")
    stats.mkdir()
    conf_path = stats / "config.yaml"
    version_path = stats / "uid.yaml"
    monkeypatch.setattr(telemetry, "DEFAULT_HOME_DIR", ".")
    conf_path.write_text("version_check_enabled: True\n")
    version_path.write_text(f"last_version_check: {last_check}\n")


def test_python_major_version():
    version = telemetry.python_version()
    major = version.split(".")[0]
    assert int(major) == 3


def test_creates_config_directory(
    monkeypatch, tmp_directory, ignore_fal_stats_enabled_env_var
):
    monkeypatch.setattr(telemetry, "DEFAULT_HOME_DIR", ".")

    # TODO: enable passing `config` object for FalDbt() object creation cases
    @telemetry.log_call("some_action")
    def my_function():
        pass

    my_function()

    assert Path("stats").is_dir()
    assert Path("stats", "uid.yaml").is_file()
    assert Path("stats", "config.yaml").is_file()


@pytest.fixture
def mock_telemetry(monkeypatch):
    mock = Mock()
    mock_dt = Mock()
    mock_dt.now.side_effect = [1, 2]
    monkeypatch.setattr(telemetry, "log_api", mock)
    monkeypatch.setattr(telemetry.datetime, "datetime", mock_dt)
    yield mock


def test_log_call_success(mock_telemetry):
    @telemetry.log_call("some_action", ["a", "c", "d", "e"])
    def my_function(a, b, c=0, *, d=1, e):
        pass

    my_function(1, 2, e=3)

    mock_telemetry.assert_has_calls(
        [
            call(
                action="some_action_started",
                additional_props={
                    "args": {"a": 1, "c": 0, "d": 1, "e": 3},
                },
                dbt_config=None,
            ),
            call(
                action="some_action_success",
                total_runtime="1",
                additional_props={
                    "args": {"a": 1, "c": 0, "d": 1, "e": 3},
                },
                dbt_config=None,
            ),
        ]
    )


def test_log_call_exception(mock_telemetry):
    @telemetry.log_call("some_action", ["a", "c", "d", "e"])
    def my_function(a, b, c=0, *, d=1, e):
        raise ValueError("some error")

    with pytest.raises(ValueError):
        my_function(1, 2, e=3)

    mock_telemetry.assert_has_calls(
        [
            call(
                action="some_action_started",
                additional_props={
                    "args": {"a": 1, "c": 0, "d": 1, "e": 3},
                },
                dbt_config=None,
            ),
            call(
                action="some_action_error",
                total_runtime="1",
                additional_props={
                    "exception": str(type(ValueError())),
                    "args": {"a": 1, "c": 0, "d": 1, "e": 3},
                },
                dbt_config=None,
            ),
        ]
    )


def test_redaction():
    with patch("posthog.capture") as mock_capture:

        # TODO: seems wrong to replace the function in the module _for real_. should be mocked
        telemetry.check_uid = lambda: ("test_uid", None, True)
        telemetry.check_stats_enabled = lambda: True
        telemetry.is_online = lambda: True
        telemetry.log_api(
            "some_action",
            additional_props={
                "argv": [
                    "/Users/user/.pyenv/shims/fal",
                    "flow",
                    "run",
                    "--disable-logging",
                    "--project-dir",
                    "some_dir",
                    "--profiles-dir",
                    "some_other_dir",
                    "--defer",
                    "--threads",
                    "--state",
                    "some_state",
                    "-s",
                    "--select",
                    "some_selector",
                    "-m",
                    "--models",
                    "some_model",
                    "--exclude",
                    "one_more_model",
                    "--selector",
                    "some_other_selector",
                    "--all",
                    "--scripts",
                    "some_script",
                    "--before",
                    "--debug",
                    "--vars",
                    "{env: 'some'}",
                    "--globals",
                ]
            },
        )

        mock_capture.assert_called_with(
            distinct_id="test_uid",
            event="some_action",
            properties={
                "tool": "fal-cli",
                "config_hash": "",
                "event_id": ANY,
                "invocation_id": ANY,
                "user_id": "test_uid",
                "action": "some_action",
                "client_time": ANY,
                "total_runtime": ANY,
                "python_version": ANY,
                "fal_version": ANY,
                "dbt_version": ANY,
                "dbt_adapter": ANY,
                "docker_container": ANY,
                "airflow": ANY,
                "github_action": ANY,
                "gitlab_ci": ANY,
                "os": ANY,
                "telemetry_version": ANY,
                "$geoip_disable": True,
                "$ip": None,
                "argv": [
                    "[REDACTED]",
                    "flow",
                    "run",
                    "--disable-logging",
                    "--project-dir",
                    "[REDACTED]",
                    "--profiles-dir",
                    "[REDACTED]",
                    "--defer",
                    "--threads",
                    "--state",
                    "[REDACTED]",
                    "-s",
                    "--select",
                    "[REDACTED]",
                    "-m",
                    "--models",
                    "[REDACTED]",
                    "--exclude",
                    "[REDACTED]",
                    "--selector",
                    "[REDACTED]",
                    "--all",
                    "--scripts",
                    "[REDACTED]",
                    "--before",
                    "--debug",
                    "--vars",
                    "[REDACTED]",
                    "--globals",
                ],
            },
        )
