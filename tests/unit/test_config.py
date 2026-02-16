from pathlib import Path

import pytest

from src.utils.config import Config


@pytest.mark.unit
def test_config_falls_back_to_repo_relative_path():
    cfg = Config("config.yaml")
    assert cfg.loaded_config_path is not None
    assert Path(cfg.loaded_config_path).exists()
    assert cfg.exchanges.binance.symbols
