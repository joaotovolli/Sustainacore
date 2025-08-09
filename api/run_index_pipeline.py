import importlib.util, pathlib, sys
from utils.log import logger

SCRIPT_DIR = pathlib.Path(__file__).parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

STEPS = [
    "01_upload_prices",
    "02_patch_missing_constituents",
    "03_generate_constituents",
    "04_calculate_index_levels",
    "05_calculate_returns",
]

def _run_step(stem):
    path = SCRIPT_DIR / f"{stem}.py"
    logger.info("-> Running %s", path.name)
    spec = importlib.util.spec_from_file_location(stem, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    if hasattr(mod,'run'):
        mod.run()

def run():
    for s in STEPS:
        _run_step(s)
if __name__ == "__main__":
    run()