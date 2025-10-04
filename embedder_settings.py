"""Embedding configuration and parity checks for Sustainacore."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Tuple

try:  # pragma: no cover - optional dependency for import-time use
    import oracledb  # type: ignore
except Exception:  # pragma: no cover - allow running without Oracle client
    oracledb = None  # type: ignore

try:
    import db_helper
except Exception:  # pragma: no cover - optional at import time
    db_helper = None  # type: ignore


LOGGER = logging.getLogger("sustainacore.embed")


@dataclass
class EmbedSettings:
    """Configuration for the embedding client."""

    model_name: str
    expected_dimension: int
    strict_parity: bool
    provider: str = "oracle_vector"
    normalization: str = "none"
    metadata_table: Optional[str] = None


@dataclass
class EmbedParityResult:
    """Result of a parity check between configuration and the corpus."""

    expected_model: str
    actual_model: Optional[str]
    expected_dimension: int
    actual_dimension: Optional[int]
    metadata: Dict[str, Optional[str]]

    def is_match(self) -> bool:
        if self.actual_dimension is None:
            return False
        if self.actual_dimension != self.expected_dimension:
            return False
        if self.actual_model and self.actual_model != self.expected_model:
            return False
        return True


class EmbedParityError(RuntimeError):
    """Raised when STRICT_EMBED_PARITY=true and the parity check fails."""


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    raw = raw.strip().lower()
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def get_embed_settings() -> EmbedSettings:
    """Return cached embedding settings parsed from environment variables."""

    model = os.getenv("EMBED_MODEL_NAME") or os.getenv("OLLAMA_EMBED_MODEL") or "all-minilm"
    try:
        expected_dim = int(os.getenv("EMBED_DIMENSION") or os.getenv("EMBED_DIM") or "384")
    except ValueError:
        expected_dim = 384
    strict = _env_bool("STRICT_EMBED_PARITY", False)
    normalization = os.getenv("EMBED_NORMALIZATION", "none")
    metadata_table = os.getenv("EMBED_METADATA_TABLE")
    return EmbedSettings(
        model_name=model.strip() or "all-minilm",
        expected_dimension=max(1, expected_dim),
        strict_parity=strict,
        normalization=normalization.strip() or "none",
        metadata_table=metadata_table.strip() if metadata_table else None,
    )


def _default_metadata_fetcher(
    table: str,
    column: str,
) -> Tuple[Optional[int], Dict[str, Optional[str]]]:
    if db_helper is None:
        raise RuntimeError("db_helper module not available")
    return db_helper.fetch_vector_metadata(table=table, column=column)


def run_startup_parity_check(
    settings: Optional[EmbedSettings] = None,
    *,
    table: str = "ESG_DOCS",
    column: str = "EMBEDDING",
    fetcher: Optional[Callable[[str, str], Tuple[Optional[int], Dict[str, Optional[str]]]]] = None,
) -> EmbedParityResult:
    """Validate the configured embedding model against the Oracle corpus."""

    cfg = settings or get_embed_settings()
    fetcher_fn = fetcher or _default_metadata_fetcher

    actual_dim: Optional[int] = None
    metadata: Dict[str, Optional[str]] = {}
    actual_model: Optional[str] = None

    try:
        actual_dim, metadata = fetcher_fn(table, column)
        actual_model = metadata.get("model_name") if metadata else None
    except EmbedParityError:
        raise
    except Exception as exc:  # pragma: no cover - defensive logging
        message = f"Embed parity probe failed: {exc}"
        if cfg.strict_parity:
            raise EmbedParityError(message) from exc
        LOGGER.warning(message)
        return EmbedParityResult(
            expected_model=cfg.model_name,
            actual_model=None,
            expected_dimension=cfg.expected_dimension,
            actual_dimension=None,
            metadata={},
        )

    result = EmbedParityResult(
        expected_model=cfg.model_name,
        actual_model=actual_model,
        expected_dimension=cfg.expected_dimension,
        actual_dimension=actual_dim,
        metadata=metadata,
    )

    log_payload = {
        "provider": cfg.provider,
        "model": cfg.model_name,
        "expected_dim": cfg.expected_dimension,
        "actual_dim": actual_dim,
        "actual_model": actual_model,
        "normalization": cfg.normalization,
    }
    LOGGER.info("Embedder startup configuration: %s", log_payload)

    if not result.is_match():
        mismatch_msg = (
            "Embedder parity mismatch detected: "
            f"expected model '{cfg.model_name}'/{cfg.expected_dimension}D, "
            f"found model '{actual_model}'/{actual_dim}D"
        )
        if cfg.strict_parity:
            raise EmbedParityError(mismatch_msg)
        LOGGER.warning(mismatch_msg)
    else:
        LOGGER.info("Embed parity verified against Oracle corpus")

    return result


__all__ = [
    "EmbedParityError",
    "EmbedParityResult",
    "EmbedSettings",
    "get_embed_settings",
    "run_startup_parity_check",
]
