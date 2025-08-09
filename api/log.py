import logging
import pathlib
import sys

log_dir = pathlib.Path(__file__).resolve().parent.parent / 'logs'
log_dir.mkdir(exist_ok=True)
log_file = log_dir / 'error_log.txt'

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding='utf-8', errors='replace')
    ]
)
logger = logging.getLogger('index')