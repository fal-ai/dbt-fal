from contextlib import contextmanager
from dbt import flags
from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.factory import FACTORY
from dbt.config.profile import Profile, read_profile
from dbt.config.renderer import ProfileRenderer
from dbt.config.utils import parse_cli_vars

from .connections import FalEncCredentials
from .wrappers import FalEncAdapterWrapper, FalCredentialsWrapper


@contextmanager
def _release_plugin_lock():
    FACTORY.lock.release()
    try:
        yield
    finally:
        FACTORY.lock.acquire()


def load_db_profile(config) -> Profile:
    fal_credentials = config.credentials

    raw_profile_data = read_profile(flags.PROFILES_DIR)
    cli_vars = parse_cli_vars(getattr(config.args, "vars", "{}"))
    with _release_plugin_lock():
        return Profile.from_raw_profiles(
            raw_profile_data,
            config.profile_name,
            renderer=ProfileRenderer(cli_vars),
            target_override=fal_credentials.db_profile,
        )


# NOTE: cls.Relation = BaseRelation, which may be problematic?
# TODO: maybe assign FalEncAdapter.Relation in `__init__` Plugin and have this directly inherit from FalAdapterMixin
class FalEncAdapter(BaseAdapter):
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
            db_profile = load_db_profile(config)
            db_credentials = db_profile.credentials
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
            db_adapter_plugin = FACTORY.get_plugin_by_name(db_credentials.type)

        db_type: BaseAdapter = db_adapter_plugin.adapter  # type: ignore

        original_plugin.dependencies = [db_credentials.type]

        config.python_adapter_credentials = fal_credentials
        config.sql_adapter_credentials = db_credentials

        with _release_plugin_lock():
            # Temporary credentials for register
            config.credentials = config.sql_adapter_credentials
            FACTORY.register_adapter(config)

        config.credentials = FalCredentialsWrapper(config.sql_adapter_credentials)

        return FalEncAdapterWrapper(db_type, config)

    @classmethod
    def type(cls):
        return "fal"
