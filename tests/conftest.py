"""Shared fixtures: isolated data dir, small limits, chatty logs."""
from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest

# Keep multipart minimum tiny so tests exercise the merge path cheaply.
os.environ.setdefault("OSG_MIN_PART_SIZE", "4")
os.environ["OSG_MAX_OBJECT_SIZE"] = str(1024 * 1024)
os.environ.setdefault("OSG_MAX_INFLIGHT_WRITES", "4")

from app.config import load_settings
from app.store import GatewayStore

log = logging.getLogger("test.fixture")


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    data = tmp_path / "osg-data"
    data.mkdir()
    log.info("fixture data_dir=%s", data)
    return data


@pytest.fixture
def settings(tmp_data_dir: Path):
    return load_settings(str(tmp_data_dir))


@pytest.fixture
async def store(settings) -> GatewayStore:
    s = GatewayStore(settings)
    report = await s.start()
    log.info("fixture store started recovery=%s", report)
    return s
