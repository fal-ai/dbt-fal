from IPython.core.magic import register_line_magic, needs_local_scope
from fal import FalDbt


@register_line_magic
@needs_local_scope
def init_fal(line="", local_ns={}):
    '''
    Init fal magic variables. Must provide project_dir and profiles_dir.

    Example:
    """
    from faldbt.magics import init_fal

    %init_fal project_dir=/my_project_dir profiles_dir=/my_profiles_dir
    """
    '''
    args = dict([arg.split("=") for arg in line.split()])
    if not args.get("project_dir") or not args.get("profiles_dir"):
        raise Exception(
            """
            Both project_dir and profiles_dir need to be provided:
            Example: %init_fal project_dir=/my_project_dir profiles_dir=/my_profiles_dir
            """
        )

    faldbt = FalDbt(args["project_dir"], args["profiles_dir"])

    fal_globals = {
        "ref": faldbt.ref,
        "source": faldbt.source,
        "write_to_source": faldbt.write_to_source,
        "write_to_firestore": faldbt.write_to_firestore,
        "list_models": faldbt.list_models,
        "list_models_ids": faldbt.list_models_ids,
        "list_sources": faldbt.list_sources,
        "list_features": faldbt.list_features,
        "el": faldbt.el,
    }

    local_ns.update(fal_globals)
