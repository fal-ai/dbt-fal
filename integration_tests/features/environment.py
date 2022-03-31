import os


def before_all(context):
    os.environ["FAL_STATS_ENABLED"] = "false"
    if not hasattr(context, "temp_dir"):
        os.system("mkdir mock/temp")
        os.system("dbt seed --profiles-dir mock --project-dir mock")


def after_all(context):
    os.system("rm -rf mock/temp")
    if hasattr(context, "temp_dir"):
        context.temp_dir.cleanup()


def after_scenario(context, scenario):
    if not hasattr(context, "temp_dir"):
        os.system("rm -rf mock/temp/*")
