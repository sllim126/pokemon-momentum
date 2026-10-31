"""Shared fixtures: a temporary data folder with a store imported from the synthetic export.

Nothing here touches data/storefront or the network; image fetching is off in tests.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from storefront.app import importer
from storefront.app.main import create_app
from storefront.app.settings import Settings

FIXTURES = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    data_dir = tmp_path / "storefront"
    data_dir.mkdir()
    shutil.copy(FIXTURES / "market_reference_sample.json", data_dir / "market_reference.json")
    return Settings(
        data_dir=data_dir,
        base_url="https://shop.test",
        allow_indexing=False,
        market_base_url="https://market.poke6s.com",
        tcgplayer_store_url="https://poke6s.tcgplayerpro.com/",
        contact_email="admin@poke6s.com",
    )


def run_import(settings: Settings, **kwargs) -> importer.ImportReport:
    return importer.run(
        FIXTURES / "squarespace_export_sample.csv",
        db_path=settings.db_path,
        market_reference=settings.market_reference_path,
        images_dir=settings.images_dir,
        mapping_csv=FIXTURES / "mapping_sample.csv",
        intake_csv=FIXTURES / "sealed_intake_sample.csv",
        **kwargs,
    )


@pytest.fixture()
def imported(settings: Settings) -> Settings:
    run_import(settings)
    return settings


@pytest.fixture()
def client(imported: Settings):
    with TestClient(create_app(imported), follow_redirects=False) as test_client:
        yield test_client
