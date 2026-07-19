import logging

from pathlib import Path

root = Path(__file__).resolve().parents[2]

log_dir = root / "logs"
log_dir.mkdir(parents=True, exist_ok=True)

log_file = log_dir / "Pipeline.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)