from typing import Optional

from collections import defaultdict
from contextlib import contextmanager
from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.protocol import AdapterConfig
from dbt.adapters.factory import FACTORY

# TODO: offer in `from isolate import is_agent`
from isolate.connections.common import is_agent

from .connections import FalEncCredentials
from .wrappers import FalEncAdapterWrapper, FalCredentialsWrapper

from .load_db_profile import load_profiles_info_1_5


class FalConfigs(AdapterConfig):
    fal_environment: Optional[str]
    fal_machine: Optional[str]


@contextmanager
def _release_plugin_lock():
    FACTORY.lock.release()
    try:
        yield
    finally:
        FACTORY.lock.acquire()


DB_PROFILE = None
DB_RELATION = BaseRelation
OVERRIDE_PROPERTIES = {}

# NOTE: Should this file run on isolate agents? Could we skip it entirely and build a FalEncAdapterWrapper directly?
if not is_agent():
    DB_PROFILE, OVERRIDE_PROPERTIES = load_profiles_info_1_5()
    DB_RELATION = FACTORY.get_relation_class_by_name(DB_PROFILE.credentials.type)


class FalEncAdapter(BaseAdapter):
    Relation = DB_RELATION  # type: ignore

    # TODO: how do we actually use this?
    AdapterSpecificConfigs = FalConfigs

    def __new__(cls, config):
        # There are two different credentials types which can be passed to FalEncAdapter
        # 1. FalEncCredentials
        # 2. FalCredentialsWrapper
        #
        # For `FalEncCredentials`, we have to go through parsing the profiles.yml (so that we
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
            db_adapter_class = FACTORY.get_adapter_class_by_name(db_credentials.type)
            original_plugin = FACTORY.get_plugin_by_name(fal_credentials.type)
            original_plugin.dependencies = [db_credentials.type]

        config.python_adapter_credentials = fal_credentials
        config.sql_adapter_credentials = db_credentials

        for key in OVERRIDE_PROPERTIES:
            if OVERRIDE_PROPERTIES[key] is not None:
                setattr(config, key, OVERRIDE_PROPERTIES[key])

        with _release_plugin_lock():
            # Temporary credentials for register
            config.credentials = config.sql_adapter_credentials
            FACTORY.register_adapter(config)
            config.credentials = FalCredentialsWrapper(config.sql_adapter_credentials)

        return FalEncAdapterWrapper(db_adapter_class, config)

    @classmethod
    def type(cls):
        return "fal"
