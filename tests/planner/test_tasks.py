import pytest
from dataclasses import dataclass, field
from fal.planner.tasks import SUCCESS, FAILURE, DBTTask, FalModelTask, FalHookTask


@dataclass
class FakeCLIOutput:
    return_code: int
    run_results: dict


@dataclass
class FakeFalDbt:
    target_path: str
    models: list = field(default_factory=list)


@dataclass
class FakeModel:
    unique_id: str


class FakeScript:
    pass


def mock_dbt_run(mocker, return_code):
    mocker.patch(
        "fal.cli.dbt_runner.dbt_run",
        return_value=FakeCLIOutput(return_code, {}),
    )
    mocker.patch(
        "fal.planner.tasks._map_cli_output_model_statuses",
        return_value=[],
    )


def mock_script_construction(mocker, return_code):
    mocker.patch(
        "fal.fal_script.FalScript.__new__",
        return_value=FakeScript(),
    )
    mocker.patch(
        "fal.fal_script.FalScript.model_script",
        return_value=FakeScript(),
    )
    mocker.patch(
        "fal.planner.tasks._run_script",
        return_value=return_code,
    )


@pytest.mark.parametrize("return_code", [SUCCESS, FAILURE])
def test_dbt_task(mocker, return_code):
    task = DBTTask(["a", "b"])
    task._run_index = 1

    fal_dbt = FakeFalDbt("/test")
    mock_dbt_run(mocker, return_code)
    assert task.execute(None, fal_dbt) == return_code


def test_fal_model_task_when_dbt_fails(mocker):
    task = FalModelTask(["a", "b"])
    task._run_index = 1

    fal_dbt = FakeFalDbt("/test")
    mock_dbt_run(mocker, FAILURE)
    assert task.execute(None, fal_dbt) == FAILURE


@pytest.mark.parametrize("return_code", [SUCCESS, FAILURE])
def test_fal_model_task_when_dbt_succeeds(mocker, return_code):
    task = FalModelTask(["a", "b"], bound_model=FakeModel("model"))
    task._run_index = 1

    fal_dbt = FakeFalDbt("/test")
    mock_dbt_run(mocker, SUCCESS)
    mock_script_construction(mocker, return_code)
    assert task.execute(None, fal_dbt) == return_code


@pytest.mark.parametrize("return_code", [SUCCESS, FAILURE])
def test_fal_hook(mocker, return_code):
    task = FalHookTask("something.py", bound_model=FakeModel("model"))
    task._run_index = 1

    fal_dbt = FakeFalDbt("/test")
    mock_script_construction(mocker, return_code)
    assert task.execute(None, fal_dbt) == return_code