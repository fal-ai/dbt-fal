import stat
import shutil
import os
import pytest
import tempfile
from glob import iglob
from pathlib import Path


def pytest_configure(config):
    """
    Allows plugins and conftest files to perform initial configuration.
    This hook is called for every plugin and initial conftest
    file after command line options have been parsed.
    """
    os.environ['FAL_STATS_ENABLED'] = 'False'

def _delete_dot_git_at(path):
    for root, dirs, files in os.walk(path):
        for dir_ in dirs:
            os.chmod(Path(root, dir_), stat.S_IRWXU)
        for file_ in files:
            os.chmod(Path(root, file_), stat.S_IRWXU)

def _delete_all_dot_git():
    if os.name == 'nt':
        for path in iglob('**/.git', recursive=True):
            _delete_dot_git_at(path)

@pytest.fixture()
def tmp_directory():
    old = os.getcwd()
    tmp = tempfile.mkdtemp()
    os.chdir(str(tmp))

    yield tmp

    # some tests create sample git repos, if we are on windows, we need to
    # change permissions to be able to delete the files
    _delete_all_dot_git()

    os.chdir(old)

    shutil.rmtree(str(tmp))
