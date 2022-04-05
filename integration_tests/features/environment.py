import os


def before_all(context):
    os.environ["FAL_STATS_ENABLED"] = "false"


def after_all(context):
    if hasattr(context, "temp_dir"):
        context.temp_dir.cleanup()
