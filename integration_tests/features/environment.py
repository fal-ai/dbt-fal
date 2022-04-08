import os


def before_all(context):
    os.environ["FAL_STATS_ENABLED"] = "false"


def after_scenario(context, scenario):
    if hasattr(context, "temp_dir"):
        context.temp_dir.cleanup()
    if hasattr(context, "added_during_tests"):
        for file in context.added_during_tests:
            os.remove(file)
