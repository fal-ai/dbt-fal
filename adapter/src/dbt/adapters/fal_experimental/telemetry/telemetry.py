"""
dbt-fal telemetry code uses source code from: https://github.com/ploomber/ploomber
"""

import datetime
import http.client as httplib
import warnings
import posthog
import pkg_resources
import yaml
import os
from pathlib import Path
import sys
import uuid
from functools import wraps
from typing import Any, List, Optional
import inspect

import platform

import atexit

TELEMETRY_VERSION = "0.0.1"
DEFAULT_HOME_DIR = "~/.fal"
CONF_DIR = "stats"
FAL_HOME_DIR = os.getenv("FAL_HOME_DIR", DEFAULT_HOME_DIR)

posthog.project_api_key = "phc_Yf1tsGPPb4POvqVjelT3rPPv2c3FH91zYURyyL30Phy"

invocation_id = uuid.uuid4()


def shutdown():
    posthog.shutdown()
    # HACK: while https://github.com/PostHog/posthog-python/pull/52 happens
    from posthog.request import _session as posthog_session

    posthog_session.close()


atexit.register(shutdown)


def str_param(item: Any) -> str:
    if not isinstance(item, str):
        raise TypeError(f"Variable not supported/wrong type: {item} should be a str")
    return item


def opt_str_param(item: Any) -> Optional[str]:
    if item is None:
        return item
    return str_param(item)


def python_version():
    py_version = sys.version_info
    return f"{py_version.major}.{py_version.minor}.{py_version.micro}"


def is_online():
    """Check if host is online"""
    conn = httplib.HTTPSConnection("www.google.com", timeout=1)

    try:
        conn.request("HEAD", "/")
        return True
    except Exception:
        return False
    finally:
        conn.close()


# Will output if the code is within a container
def is_docker():
    try:
        cgroup = Path("/proc/self/cgroup")
        docker_env = Path("/.dockerenv")
        return (
            docker_env.exists()
            or cgroup.exists()
            and any("docker" in line for line in cgroup.read_text().splitlines())
        )
    except OSError:
        return False


def is_github():
    """Return True if inside a GitHub Action"""
    return os.getenv("GITHUB_ACTIONS") is not None


def is_gitlab():
    """Return True if inside a GitLab CI"""
    return os.getenv("GITLAB_CI") is not None


def get_os():
    """
    The function will output the client platform
    """
    return platform.system()


def dbt_installed_version():
    """Returns: dbt version"""
    try:
        return pkg_resources.get_distribution("dbt-core").version
    except ImportError:
        return


def fal_installed_version():
    """Returns: fal version"""
    try:
        return pkg_resources.get_distribution("fal").version
    except ImportError:
        return


def is_airflow():
    """Returns: True for Airflow env"""
    return "AIRFLOW_CONFIG" in os.environ or "AIRFLOW_HOME" in os.environ


def get_home_dir():
    """
    Checks if fal home was set through the env variable.
    returns the actual home_dir path.
    """
    return FAL_HOME_DIR if FAL_HOME_DIR else DEFAULT_HOME_DIR


def check_dir_exist(input_location=None):
    """
    Checks if a specific directory exists, creates if not.
    In case the user didn't set a custom dir, will turn to the default home
    """
    home_dir = get_home_dir()

    if input_location:
        p = Path(home_dir, input_location)
    else:
        p = Path(home_dir)

    p = p.expanduser()

    if not p.exists():
        p.mkdir(parents=True)

    return p


def check_uid():
    """
    Checks if local user id exists as a uid file, creates if not.
    """
    uid_path = Path(check_dir_exist(CONF_DIR), "uid.yaml")
    conf = read_conf_file(uid_path)  # file already exist due to version check
    if "uid" not in conf.keys():
        uid = str(uuid.uuid4())
        res = write_conf_file(uid_path, {"uid": uid}, error=True)
        if res:
            return "NO_UID", res, True
        else:
            return uid, None, True

    return conf.get("uid") or "NO_UID", None, False


def check_stats_enabled():
    """
    Check if the user allows us to use telemetry. In order of precedence:
    1. If FAL_STATS_ENABLED is defined, check its value
    2. If DO_NOT_TRACK is defined, check its value
    3. Otherwise use the value in stats_enabled in the config.yaml file
    """
    if "FAL_STATS_ENABLED" in os.environ:
        val = os.environ["FAL_STATS_ENABLED"].lower().strip()
        return val != "0" and val != "false" and val != ""

    if "DO_NOT_TRACK" in os.environ:
        val = os.environ["DO_NOT_TRACK"].lower().strip()
        return val != "1" and val != "true"

    # Check if local config exists
    config_path = Path(check_dir_exist(CONF_DIR), "config.yaml")
    if not config_path.exists():
        write_conf_file(config_path, {"stats_enabled": True})
        return True
    else:  # read and return config
        conf = read_conf_file(config_path)
        return conf.get("stats_enabled", True)


def check_first_time_usage():
    """
    The function checks for first time usage if the conf file exists and the
    uid file doesn't exist.
    """
    config_path = Path(check_dir_exist(CONF_DIR), "config.yaml")
    uid_path = Path(check_dir_exist(CONF_DIR), "uid.yaml")
    uid_conf = read_conf_file(uid_path)
    return config_path.exists() and "uid" not in uid_conf.keys()


def read_conf_file(conf_path):
    try:
        with conf_path.open("r") as file:
            conf = yaml.safe_load(file)
            return conf
    except Exception as e:
        warnings.warn(f"Can't read config file {e}")
        return {}


def write_conf_file(conf_path, to_write, error=None):
    try:  # Create for future runs
        with conf_path.open("w") as file:
            yaml.dump(to_write, file)
    except Exception as e:
        warnings.warn(f"Can't write to config file: {e}")
        if error:
            return e


def log_api(
    action: str,
    total_runtime=None,
    config=None,
    additional_props: Optional[dict] = None,
):
    """
    This function logs through an API call, assigns parameters if missing like
    timestamp, event id and stats information.
    """

    if not check_stats_enabled():
        return

    if not is_online():
        return

    additional_props = additional_props or {}

    event_id = uuid.uuid4()

    client_time = datetime.datetime.now()

    uid, uid_error, is_install = check_uid()

    if "NO_UID" in uid:
        additional_props["uid_issue"] = str(uid_error) if uid_error is not None else ""

    config_hash = ""
    if config is not None and hasattr(config, "hashed_name"):
        config_hash = str(config.hashed_name())

    opt_str_param(uid)
    str_param(action)

    props = {
        "tool": "dbt-fal",
        "config_hash": config_hash,
        "event_id": str(event_id),
        "invocation_id": str(invocation_id),
        "user_id": uid,
        "action": action,
        "client_time": str(client_time),
        "total_runtime": str(total_runtime),
        "python_version": python_version(),
        "dbt_version": dbt_installed_version(),
        "docker_container": is_docker(),
        "airflow": is_airflow(),
        "github_action": is_github(),
        "gitlab_ci": is_gitlab(),
        "os": get_os(),
        "telemetry_version": TELEMETRY_VERSION,
        "$geoip_disable": True,  # This disables GeoIp despite the backend setting
        "$ip": None,  # This disables IP tracking
    }

    all_props = {**props, **additional_props}

    if "argv" in all_props:
        all_props["argv"] = _clean_args_list(all_props["argv"])

    if is_install:
        posthog.capture(distinct_id=uid, event="install_success", properties=all_props)

    posthog.capture(distinct_id=uid, event=action, properties=all_props)


def log_call(action, log_args: List[str] = [], config: bool = False):
    """Runs a function and logs it"""

    def _log_call(func):
        @wraps(func)
        def wrapper(*func_args, **func_kwargs):

            sig = inspect.signature(func).bind(*func_args, **func_kwargs)
            sig.apply_defaults()
            log_args_props = dict(
                map(lambda arg: (arg, sig.arguments.get(arg)), log_args)
            )

            func_self = func_args[0]
            # Get the dbt config from the self object of the method
            dbt_config = func_self.config if config else None

            log_api(
                action=f"{action}_started",
                additional_props={
                    "argv": sys.argv,
                    "args": log_args_props,
                },
                config=dbt_config,
            )

            start = datetime.datetime.now()

            try:
                result = func(*func_args, **func_kwargs)
            except Exception as e:
                log_api(
                    action=f"{action}_error",
                    total_runtime=str(datetime.datetime.now() - start),
                    additional_props={
                        "exception": str(type(e)),
                        "argv": sys.argv,
                        "args": log_args_props,
                    },
                    config=dbt_config,
                )
                raise
            else:
                log_api(
                    action=f"{action}_success",
                    total_runtime=str(datetime.datetime.now() - start),
                    additional_props={
                        "argv": sys.argv,
                        "args": log_args_props,
                    },
                    config=dbt_config,
                )

            return result

        return wrapper

    return _log_call


def _clean_args_list(args: List[str]) -> List[str]:
    ALLOWLIST = [
        "--disable-logging",
        "--project-dir",
        "--profiles-dir",
        "--defer",
        "--threads",
        "--thread",
        "--state",
        "--full-refresh",
        "-s",
        "--select",
        "-m",
        "--models",
        "--model",
        "--exclude",
        "--selector",
        "--all",
        "run",
        "dbt",
        "-v",
        "--version",
        "--debug",
        "--vars",
        "--var",
        "--target",
        "build",
        "clean",
        "compile",
        "debug",
        "deps",
        "docs",
        "init",
        "list",
        "parse",
        "seed",
        "snapshot",
        "source",
        "test",
        "rpc",
        "run-operation",
    ]
    REDACTED = "[REDACTED]"
    output = []
    for item in args:
        if item in ALLOWLIST:
            output.append(item)
        else:
            output.append(REDACTED)
    return output
