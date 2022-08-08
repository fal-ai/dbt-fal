from platformdirs import user_cache_dir
from pathlib import Path

BASE_CACHE_DIR = Path(user_cache_dir("fal", "fal"))
BASE_CACHE_DIR.mkdir(exist_ok=True)

