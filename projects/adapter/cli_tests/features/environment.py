from behave.configuration import Configuration
import os


def before_all(context):
    os.environ["FAL_STATS_ENABLED"] = "false"
    config: Configuration = context.config
    config.setup_logging()


def after_scenario(context, scenario):
    if hasattr(context, "temp_dir"):
        context.temp_dir.cleanup()

    if hasattr(context, "added_during_tests"):
        for file in context.added_during_tests:

            os.remove(file)
        delattr(context, "added_during_tests")

    if hasattr(context, "exc") and context.exc:
        from traceback import print_exception

        _etype, exception, _tb = context.exc

        print_exception(*context.exc)

        raise AssertionError("Should have expected exception") from exception


def before_tag(context, tag):
    if "TODO-logging" == tag:
        # print here is not captured by behave
        print("WARN: should have thrown an exception (TODO-logging)")
    elif "requires-conda" == tag:
        # See if conda is available, and if not skip the
        # current scenerio.
        from fal.dbt.packages.environments.conda import get_conda_executable

        try:
            executable = get_conda_executable()
        except RuntimeError:
            context.scenario.skip(
                reason="this test requires conda, but conda is not installed."
            )
