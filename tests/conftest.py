import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from gateway.config import Settings
from main import create_app

ALICE = {"X-Identity": "alice"}
BOB = {"X-Identity": "bob"}


@pytest.fixture
def settings(tmp_path):
    return Settings(data_dir=tmp_path / "data", min_part_size=1)


@pytest.fixture
def client(settings):
    return TestClient(create_app(settings))


@pytest.fixture
def make_client(tmp_path):
    """Factory: build a client with custom settings overrides."""
    def _make(**overrides):
        s = Settings(data_dir=tmp_path / "data", min_part_size=1, **overrides)
        return TestClient(create_app(s)), s
    return _make


@pytest.fixture
def thread_client(client):
    """Factory for genuinely concurrent requests: a TestClient serialises
    calls made through one shared portal, so concurrency tests must hand
    each thread its own client bound to the same app."""
    app = client.app

    def _make():
        return TestClient(app)

    return _make
