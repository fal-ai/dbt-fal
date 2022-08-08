from __future__ import annotations

import hashlib
import threading
from collections import defaultdict
from dataclasses import dataclass
from typing import (
    Dict,
    Any,
    ClassVar,
    DefaultDict,
)
from pathlib import Path

import yaml
from platformdirs import user_cache_dir

from fal.packages.base import BASE_CACHE_DIR

_BASE_REPO_DIR = BASE_CACHE_DIR / "repos"
_BASE_REPO_DIR.mkdir(exist_ok=True)

_FAL_HOOKS_FILES = [".fal-hooks.yml", ".fal-hooks.yaml"]


@dataclass
class Package:
    url: str

    branch_or_tag: str

    # A in-process mapping of packages to locks. All git operations
    # must be done under this lock to prevent race conditions.
    _GIT_LOCKS: ClassVar[DefaultDict[threading.Lock]] = defaultdict(threading.Lock)

    def __post_init__(self):
        self.name = f"{self.url}@{self.branch_or_tag}"
        self._key = hashlib.sha256(self.name.encode()).hexdigest()

    @property
    def path(self) -> Path:
        return _BASE_REPO_DIR / self._key

    def clone(self) -> None:
        from dulwich import porcelain

        def _is_constant_point():
            return self.branch_or_tag.encode() in porcelain.tag_list(self.path)

        with self._GIT_LOCKS[self._key]:
            if self.path.exists():
                # If we have the package+revision already cloned,
                # we'll check whether it is a constant point (a tag)
                # and use the cached version.
                if not _is_constant_point():
                    # However if the user specified a variable point (a branch)
                    # then we'll always get the latest revision from the remote.

                    # TODO(optimization): we probably don't have to do a full pull
                    # here (maybe just try to get a fetch with depth 1?)

                    # TODO(optimization): we should limit the number of pull's per
                    # key per fal flow run to 1.
                    porcelain.pull(self.path)
            else:
                porcelain.clone(self.url, self.path, branch=self.revision.encode())

    def load_spec(self) -> Dict[str, Any]:
        if not self.path.exists():
            self.clone()

        fal_hooks_paths = [
            self.path / file_name
            for file_name in _FAL_HOOKS_FILES
            if (self.path / file_name).exists()
        ]
        if len(fal_hooks_paths) == 0:
            raise RuntimeError(
                f"{self.name} is not a fal package (no .fal-hooks.yml/yaml file)"
            )
        elif len(fal_hooks_paths) > 1:
            raise RuntimeError(
                f"Multiple hook index files exist on {self.name!r}, "
                f"but only one is allowed."
            )

        [fal_hooks_path] = fal_hooks_paths
        with open(fal_hooks_path) as stream:
            # TODO: implement schema validation
            return yaml.safe_load(stream)
