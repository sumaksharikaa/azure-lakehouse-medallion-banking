import os
import sys
import time
from pathlib import Path

import pytest

# Spark runs in UTC; make Python-side datetimes match regardless of the machine's zone.
os.environ["TZ"] = "UTC"
time.tzset()

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "data_generator"))

DELTA_ENABLED = os.environ.get("LAKEHOUSE_DELTA_TESTS") == "1"


def pytest_collection_modifyitems(config, items):
    if DELTA_ENABLED:
        return
    skip = pytest.mark.skip(reason="set LAKEHOUSE_DELTA_TESTS=1 to run Delta integration tests")
    for item in items:
        if "delta" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
    os.environ.setdefault("SPARK_MASTER", "local[2]")
    from lakehouse.spark import get_spark

    session = get_spark("lakehouse-tests", enable_delta=DELTA_ENABLED)
    yield session
    session.stop()


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return ROOT
