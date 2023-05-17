from typing import Dict, Any, Tuple, Optional

from dbt.flags import get_flags, Namespace
from dbt.config.project import load_raw_project
from dbt.config.profile import read_profile, Profile
from dbt.config.renderer import ProfileRenderer


# NOTE: inspired in https://github.com/dbt-labs/dbt-core/blob/828d723512fced809c63e369a82c7eb570a74986/core/dbt/config/runtime.py#L58-L74
def find_profile_name(
    profile_override: Optional[str],
    project_root: str,
    profile_renderer: ProfileRenderer,
):
    if profile_override is not None:
        profile_name = profile_override
    else:
        raw_project = load_raw_project(project_root)
        raw_profile_name = raw_project.get("profile")
        profile_name = profile_renderer.render_value(raw_profile_name)

    return profile_name


# NOTE: inspired in https://github.com/dbt-labs/dbt-core/blob/828d723512fced809c63e369a82c7eb570a74986/core/dbt/config/profile.py#L279-L311
def find_target_name(
    target_override: Optional[str], raw_profile: dict, profile_renderer: ProfileRenderer
):
    if target_override is not None:
        target_name = target_override
    elif "target" in raw_profile:
        # render the target if it was parsed from yaml
        target_name = profile_renderer.render_value(raw_profile["target"])
    else:
        target_name = "default"

    return target_name


def load_profiles_info_1_5() -> Tuple[Profile, Dict[str, Any]]:
    flags: Namespace = get_flags()

    profile_renderer = ProfileRenderer(getattr(flags, "VARS", {}))

    profile_name = find_profile_name(flags.PROFILE, flags.PROJECT_DIR, profile_renderer)

    raw_profiles = read_profile(flags.PROFILES_DIR)
    raw_profile = raw_profiles[profile_name]

    target_name = find_target_name(flags.TARGET, raw_profile, profile_renderer)

    fal_dict = Profile._get_profile_data(
        profile=raw_profile,
        profile_name=profile_name,
        target_name=target_name,
    )
    db_profile_target_name = fal_dict.get("db_profile")
    assert (
        db_profile_target_name
    ), "fal credentials must have a `db_profile` property set"

    try:
        db_profile = Profile.from_raw_profile_info(
            raw_profile=raw_profile,
            profile_name=profile_name,
            renderer=profile_renderer,
            # TODO: should we load the user_config?
            user_config={},
            target_override=db_profile_target_name,
        )
    except RecursionError as error:
        raise AttributeError(
            "Did you wrap a type 'fal' profile with another type 'fal' profile?"
        ) from error

    override_properties = {
        "threads": getattr(flags, "THREADS", None) or fal_dict.get("threads") or db_profile.threads,
    }

    return db_profile, override_properties
