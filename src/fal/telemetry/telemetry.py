"""
fal telemetry code uses source code from:

https://github.com/ploomber/ploomber

Modifications are made to ensure that the code works with fal.
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
from typing import Any, Optional, Dict

import platform


TELEMETRY_VERSION = "0.0.1"
DEFAULT_HOME_DIR = "~/.fal"
CONF_DIR = "stats"
posthog.project_api_key = "phc_Yf1tsGPPb4POvqVjelT3rPPv2c3FH91zYURyyL30Phy"
FAL_HOME_DIR = os.getenv("FAL_HOME_DIR")

# Validate the input of type string
def str_param(item: Any, name: str) -> str:
    if not isinstance(item, str):
        raise TypeError(
            f"TypeError: Variable not supported/wrong type: "
            f"{item}, {name}, should be a str"
        )
    return item


def shutdown():
    posthog.shutdown()
    # HACK: while https://github.com/PostHog/posthog-python/pull/52 happens
    from posthog.request import _session as posthog_session

    posthog_session.close()


def opt_str_param(item: Any, name: str) -> Optional[str]:
    # Can leverage regular string function
    if item is not None and not isinstance(item, str):
        raise TypeError(
            f"TypeError: Variable not supported/wrong type: "
            f"{item}, {name}, should be a str"
        )
    return item


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
            return f"NO_UID {res}"
        else:
            return uid
    return conf.get("uid", "NO_UID")


def check_stats_enabled():
    """
    Check if the user allows us to use telemetry. In order of precedence:
    1. If FAL_STATS_ENABLED defined, check its value
    2. Otherwise use the value in stats_enabled in the config.yaml file
    """
    if "FAL_STATS_ENABLED" in os.environ:
        return os.environ["FAL_STATS_ENABLED"].lower() == "true"

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


def _get_telemetry_info():
    """
    The function checks for the local config and uid files, returns the right
    values according to the config file (True/False). In addition it checks
    for first time installation.
    """
    # Check if telemetry is enabled, if not skip, else check for uid
    telemetry_enabled = check_stats_enabled()

    if telemetry_enabled:

        # Check first time install
        is_install = check_first_time_usage()

        # if not uid, create
        uid = check_uid()
        return telemetry_enabled, uid, is_install
    else:
        return False, "", False


def validate_entries(event_id, uid, action, client_time, total_runtime):
    event_id = str_param(str(event_id), "event_id")
    uid = str_param(uid, "uid")
    action = str_param(action, "action")
    client_time = str_param(str(client_time), "client_time")
    total_runtime = opt_str_param(str(total_runtime), "total_runtime")
    return event_id, uid, action, client_time, total_runtime


def log_api(action, client_time=None, total_runtime=None, additional_props=None):
    """
    This function logs through an API call, assigns parameters if missing like
    timestamp, event id and stats information.
    """
    additional_props = additional_props or {}

    event_id = uuid.uuid4()
    if client_time is None:
        client_time = datetime.datetime.now()

    (telemetry_enabled, uid, is_install) = _get_telemetry_info()
    if "NO_UID" in uid:
        additional_props["uid_issue"] = uid
        uid = None

    py_version = python_version()
    docker_container = is_docker()
    os = get_os()
    online = is_online()

    if telemetry_enabled and online:
        (event_id, uid, action, client_time, total_runtime) = validate_entries(
            event_id, uid, action, client_time, total_runtime
        )
        props = {
            "event_id": event_id,
            "user_id": uid,
            "action": action,
            "client_time": str(client_time),
            "total_runtime": total_runtime,
            "python_version": py_version,
            "fal_version": fal_installed_version(),
            "dbt_version": dbt_installed_version(),
            "docker_container": docker_container,
            "os": os,
            "telemetry_version": TELEMETRY_VERSION,
            "$geoip_disable": True,  # This disables GeoIp despite the backend setting
            "$ip": None,  # This disables IP tracking
        }

        if is_airflow():
            props["airflow"] = True

        all_props = {**props, **additional_props}

        all_props = _remove_path_from_props(all_props)

        if is_install:
            posthog.capture(
                distinct_id=uid, event="install_success", properties=all_props
            )

        posthog.capture(distinct_id=uid, event=action, properties=all_props)


# NOTE: should we log differently depending on the error type?
# NOTE: how should we handle chained exceptions?
def log_call(action):
    """Runs a function and logs it"""

    def _log_call(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            log_api(action=f"{action}_started", additional_props={"argv": sys.argv})

            start = datetime.datetime.now()

            try:
                result = func(*args, **kwargs)
            except Exception as e:
                log_api(
                    action=f"{action}_error",
                    total_runtime=str(datetime.datetime.now() - start),
                    additional_props={
                        # can we log None to posthog?
                        "exception": type(e),
                        "argv": sys.argv,
                    },
                )
                raise e
            else:
                log_api(
                    action=f"{action}_success",
                    total_runtime=str(datetime.datetime.now() - start),
                    additional_props={"argv": sys.argv},
                )

            return result

        return wrapper

    return _log_call


def _remove_path_from_props(props: Dict) -> Dict:
    PATH_STR = "[PATH]"

    def _replace_value(val):
        if isinstance(val, str):
            if os.path.exists(val):
                val = PATH_STR
        elif isinstance(val, list):
            val = list(map(_replace_value, val))
        return val

    output = {key: _replace_value(value) for key, value in props.items()}
    return output
