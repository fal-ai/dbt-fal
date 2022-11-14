from typing import Dict, Any

from contextlib import contextmanager
from dbt.adapters.base.impl import BaseAdapter, BaseRelation
from dbt.adapters.factory import FACTORY

# TODO: offer in `from isolate import is_agent`
from isolate.connections.common import is_agent

from .connections import FalEncCredentials
from .wrappers import FalEncAdapterWrapper, FalCredentialsWrapper


@contextmanager
def _release_plugin_lock():
    FACTORY.lock.release()
    try:
        yield
    finally:
        FACTORY.lock.acquire()


def load_db_profile():
    import os

    from dbt.config.profile import Profile, read_profile
    from dbt.config.project import Project
    from dbt.config.renderer import ProfileRenderer
    from dbt.config.utils import parse_cli_vars
    from dbt.main import _build_base_subparser
    from dbt import flags

    # includes vars, profile, target
    parser = _build_base_subparser()
    args, _unknown = parser.parse_known_args()

    # dbt-core does os.chdir(project_dir) before reaching this location
    # from https://github.com/dbt-labs/dbt-core/blob/73116fb816498c4c45a01a2498199465202ec01b/core/dbt/task/base.py#L186
    project_root = os.getcwd()

    # from https://github.com/dbt-labs/dbt-core/blob/19c48e285ec381b7f7fa2dbaaa8d8361374136ba/core/dbt/config/runtime.py#L193-L203
    version_check = bool(flags.VERSION_CHECK)
    partial = Project.partial_load(project_root, verify_version=version_check)

    cli_vars: Dict[str, Any] = parse_cli_vars(getattr(args, "vars", "{}"))
    profile_renderer = ProfileRenderer(cli_vars)
    project_profile_name = partial.render_profile_name(profile_renderer)

    # from https://github.com/dbt-labs/dbt-core/blob/19c48e285ec381b7f7fa2dbaaa8d8361374136ba/core/dbt/config/profile.py#L423-L425
    profile_name = Profile.pick_profile_name(
        getattr(args, "profile", None), project_profile_name
    )
    raw_profiles = read_profile(flags.PROFILES_DIR)
    raw_profile = raw_profiles[profile_name]

    # from https://github.com/dbt-labs/dbt-core/blob/19c48e285ec381b7f7fa2dbaaa8d8361374136ba/core/dbt/config/profile.py#L287-L293
    target_override = getattr(args, "target", None)
    if target_override is not None:
        target_name = target_override
    elif "target" in raw_profile:
        target_name = profile_renderer.render_value(raw_profile["target"])
    else:
        target_name = "default"

    fal_dict = Profile._get_profile_data(raw_profile, profile_name, target_name)
    db_profile = fal_dict.get("db_profile")
    assert db_profile, "fal credentials must have a `db_profile` property set"

    try:
        return Profile.from_raw_profiles(
            raw_profiles,
            profile_name,
            renderer=ProfileRenderer(cli_vars),
            target_override=db_profile,
        )
    except AttributeError as error:
        if "circular import" in str(error):
            raise AttributeError(
                "Do not wrap a type 'fal' profile with another type 'fal' profile"
            ) from error


DB_PROFILE = None
DB_RELATION = BaseRelation

# NOTE: Should this file run on isolate agents? Could we skip it entirely and build a FalEncAdapterWrapper directly?
if not is_agent():
    DB_PROFILE = load_db_profile()
    DB_RELATION = FACTORY.get_relation_class_by_name(DB_PROFILE.credentials.type)


class FalEncAdapter(BaseAdapter):
    Relation = DB_RELATION  # type: ignore

    def __new__(cls, config):
        # There are two different credentials types which can be passed to FalEncAdapter
        # 1. FalEncCredentials
        # 2. FalCredentialsWrapper
        #
        # For the first one, we have to go through parsing the profiles.yml (so that we
        # can obtain the real 'db' credentials). But for the other one, we can just use
        # the bound credentials directly (e.g. in isolated mode where we don't actually
        # have access to the profiles.yml file).

        fal_credentials = config.credentials
        if isinstance(fal_credentials, FalEncCredentials):
            db_credentials = DB_PROFILE.credentials
        else:
            # Since profile construction (in the case above) already registers the
            # adapter plugin for the db type, we need to also mimic that here.
            assert isinstance(fal_credentials, FalCredentialsWrapper)
            db_credentials = fal_credentials._db_creds
            with _release_plugin_lock():
                FACTORY.load_plugin(db_credentials.type)

        # TODO: maybe we can do this better?
        with _release_plugin_lock():
            original_plugin = FACTORY.get_plugin_by_name(fal_credentials.type)
            db_adapter_class = FACTORY.get_adapter_class_by_name(db_credentials.type)

        original_plugin.dependencies = [db_credentials.type]

        config.python_adapter_credentials = fal_credentials
        config.sql_adapter_credentials = db_credentials
        config.threads = DB_PROFILE.threads

        with _release_plugin_lock():
            # Temporary credentials for register
            config.credentials = config.sql_adapter_credentials
            FACTORY.register_adapter(config)
            config.credentials = FalCredentialsWrapper(config.sql_adapter_credentials)

        return FalEncAdapterWrapper(db_adapter_class, config)

    @classmethod
    def type(cls):
        return "fal"
