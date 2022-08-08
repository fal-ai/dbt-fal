import pytest
from unittest.mock import MagicMock
from fal.packages import package
from fal.packages.package import Package, _FAL_HOOKS_FILES

EXAMPLE_HOOK = """
-   id: calculator
    name: Calculate something and print the result
    path: src/calculator.py
"""


class FakePackage(Package):
    def __init__(self, temp_dir, *args, **kwargs):
        self.temp_dir = temp_dir
        super().__init__(*args, **kwargs)

    @property
    def path(self):
        return self.temp_dir


@pytest.fixture
def fake_package(tmp_path):
    return FakePackage(tmp_path, "github.com/$$$/fake-package", "v1.0.0")


@pytest.fixture
def real_package(tmp_path):
    # TODO: create a test project on the fal-ai org
    return Package("github.com/fal-ai/fal", "v0.4.0")


@pytest.mark.parametrize("index_file_name", _FAL_HOOKS_FILES)
def test_index_loading(fake_package: FakePackage, index_file_name: str):
    with open(fake_package.path / index_file_name, "w") as stream:
        stream.write(EXAMPLE_HOOK)

    spec = fake_package.load_spec()
    assert len(spec) == 1
    assert spec[0]["id"] == "calculator"


def test_multiple_index_files(fake_package: FakePackage):
    assert len(_FAL_HOOKS_FILES) > 1
    for index_file_name in _FAL_HOOKS_FILES:
        with open(fake_package.path / index_file_name, "w") as stream:
            stream.write(EXAMPLE_HOOK)

    with pytest.raises(Exception, match="only one is allowed"):
        fake_package.load_spec()


def test_no_index_file(fake_package: FakePackage):
    with pytest.raises(Exception, match="not a fal package"):
        fake_package.load_spec()
