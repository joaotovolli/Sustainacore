import logging
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from embedder_settings import (
    EmbedParityError,
    EmbedSettings,
    run_startup_parity_check,
)


def test_strict_parity_mismatch_raises():
    settings = EmbedSettings(
        model_name="all-minilm",
        expected_dimension=384,
        strict_parity=True,
        normalization="none",
        provider="oracle_vector",
        metadata_table=None,
    )

    with pytest.raises(EmbedParityError):
        run_startup_parity_check(settings, fetcher=lambda t, c: (256, {"model_name": "all-minilm"}))


def test_non_strict_parity_logs_warning(caplog):
    settings = EmbedSettings(
        model_name="all-minilm",
        expected_dimension=384,
        strict_parity=False,
        normalization="none",
        provider="oracle_vector",
        metadata_table=None,
    )

    with caplog.at_level(logging.WARNING):
        result = run_startup_parity_check(settings, fetcher=lambda t, c: (512, {"model_name": "all-minilm"}))

    assert not result.is_match()
    assert any("parity mismatch" in message for message in caplog.text.splitlines())
