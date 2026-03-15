"""
Shared pytest fixtures and path setup for both unit and integration tests.

sys.path is configured here so every test file can import from src/ and the
framework src/ without installing anything.
"""
import sys
import os
from pathlib import Path

# Make src/ (config, workers, service, instances) importable.
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "src"))
sys.path.insert(0, str(BASE_DIR.parent / "src"))   # framework/

# ---------------------------------------------------------------------------
# Shared constants (used in both unit and integration tests)
# ---------------------------------------------------------------------------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DB_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('DB_USER','qf')}:{os.getenv('DB_PASSWORD','qf')}"
    f"@{os.getenv('DB_HOST','localhost')}:{os.getenv('DB_PORT','5432')}"
    f"/{os.getenv('DB_NAME','qf')}"
)
