import os


def before_all(context):
    os.system("mkdir mock/temp")
    os.system("dbt seed --profiles-dir mock --project-dir mock")


def after_all(context):
    os.system("rm -rf mock/temp")


def after_scenario(context, scenario):
    os.system("rm -rf mock/temp/*")
