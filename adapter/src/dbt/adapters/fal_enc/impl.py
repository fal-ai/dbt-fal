from contextlib import contextmanager
from dbt import flags
from dbt.adapters.base.impl import BaseAdapter
from dbt.adapters.factory import FACTORY
from dbt.config.profile import Profile, read_profile
from dbt.config.renderer import ProfileRenderer
from dbt.config.utils import parse_cli_vars

from .mixins import FalEncAdapterMixin, FalTypeCredentialMixin


@contextmanager
def _release_plugin_lock():
    FACTORY.lock.release()
    try:
        yield
    finally:
        FACTORY.lock.acquire()


def load_db_profile(config):
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
class FalEncAdapter(BaseAdapter):
    def __new__(cls, config):
        db_profile = load_db_profile(config)
        db_credentials = db_profile.credentials

        fal_credentials = config.credentials

        with _release_plugin_lock():
            original_plugin = FACTORY.get_plugin_by_name(fal_credentials.type)
            db_adapter_plugin = FACTORY.get_plugin_by_name(db_credentials.type)

        db_type = db_adapter_plugin.adapter
        db_creds_type = db_adapter_plugin.credentials

        original_plugin.dependencies = [db_credentials.type]

        dynamic_credentials_type = type(
            f"FalWrapper_{db_type.__name__}",
            (FalTypeCredentialMixin, db_creds_type),
            {},
        )

        raw_credential_data = {
            key: value
            for key, value in db_credentials.to_dict().items()
            if key not in db_credentials._ALIASES
        }

        config.db_credentials = db_creds_type(**raw_credential_data)
        config.python_adapter_credentials = fal_credentials


        with _release_plugin_lock():
            config.credentials = config.db_credentials
            FACTORY.register_adapter(config)
            config.credentials = dynamic_credentials_type(**raw_credential_data)

        return FalEncAdapterMixin(config, db_type)

    @classmethod
    def type(cls):
        return "fal_enc"
