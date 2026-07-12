import datetime as dt

import pytest

from app.index_engine.corporate_actions import (
    ADJUSTED_PRICE_METHOD,
    ConfirmedCorporateAction,
    adjusted_basis_is_consistent,
    detect_split_candidate,
    earliest_material_change,
    should_apply_share_adjustment,
)
from tools.index_engine import calc_index


@pytest.mark.parametrize("ratio", [2.0, 4.0])
def test_forward_split_candidates(ratio: float) -> None:
    candidate = detect_split_candidate(
        ticker="AAA", effective_date=dt.date(2026, 7, 2), previous_price=400.0,
        current_price=400.0 / ratio,
    )
    assert candidate is not None
    assert candidate.action_type == "FORWARD_SPLIT"
    assert candidate.ratio == ratio
    assert candidate.status == "PENDING"


def test_reverse_split_candidate() -> None:
    candidate = detect_split_candidate(
        ticker="AAA", effective_date=dt.date(2026, 7, 2), previous_price=10.0, current_price=40.0,
    )
    assert candidate is not None
    assert candidate.action_type == "REVERSE_SPLIT"
    assert candidate.ratio == 4.0


def test_legitimate_ordinary_move_is_not_classified_as_split() -> None:
    assert detect_split_candidate(
        ticker="AAA", effective_date=dt.date(2026, 7, 2), previous_price=100.0, current_price=76.0,
    ) is None


def test_reviewed_rebuild_allows_and_audits_non_split_large_move(capsys) -> None:
    calc_index.validate_price_return_sanity(
        prev_date=dt.date(2025, 4, 8),
        trade_date=dt.date(2025, 4, 9),
        tickers=["AAA"],
        prices_prev={"AAA": 151.29648},
        prices_now={"AAA": 183.2018},
        max_abs_return=0.20,
        action_lookup=lambda *_: None,
        allow_reviewed_non_split_moves=True,
    )
    assert "index_calc_reviewed_non_split_move" in capsys.readouterr().err


def test_reviewed_rebuild_still_blocks_unresolved_split_candidate() -> None:
    with pytest.raises(calc_index.IndexValidationError, match="corporate_action_confirmation_required"):
        calc_index.validate_price_return_sanity(
            prev_date=dt.date(2026, 7, 1),
            trade_date=dt.date(2026, 7, 2),
            tickers=["AAA"],
            prices_prev={"AAA": 400.0},
            prices_now={"AAA": 100.0},
            max_abs_return=0.20,
            action_lookup=lambda *_: None,
            allow_reviewed_non_split_moves=True,
        )


def test_adjusted_history_must_be_economically_continuous() -> None:
    action = ConfirmedCorporateAction("AAA", dt.date(2026, 7, 2), "FORWARD_SPLIT", 4.0)
    assert adjusted_basis_is_consistent(
        previous_adjusted_price=100.0, current_adjusted_price=101.0,
        confirmed_action=action, max_economic_return=0.20,
    )
    assert not adjusted_basis_is_consistent(
        previous_adjusted_price=400.0, current_adjusted_price=101.0,
        confirmed_action=action, max_economic_return=0.20,
    )
    assert not should_apply_share_adjustment(action)
    assert action.processing_method == ADJUSTED_PRICE_METHOD


def test_earliest_material_change_drives_rebuild_scope() -> None:
    days = [dt.date(2026, 6, 29), dt.date(2026, 6, 30), dt.date(2026, 7, 1)]
    stored = dict(zip(days, [100.0, 101.0, 102.0]))
    incoming = dict(zip(days, [100.0, 25.25, 25.5]))
    assert earliest_material_change(stored, incoming) == days[1]


def test_unresolved_candidate_is_recorded_and_blocks() -> None:
    recorded=[]
    with pytest.raises(calc_index.IndexValidationError, match="corporate_action_confirmation_required"):
        calc_index.validate_price_return_sanity(
            prev_date=dt.date(2026, 7, 1), trade_date=dt.date(2026, 7, 2), tickers=["AAA"],
            prices_prev={"AAA":400.0}, prices_now={"AAA":100.0}, max_abs_return=0.20,
            action_lookup=lambda *_: None, candidate_recorder=recorded.append,
        )
    assert len(recorded)==1


def test_confirmed_action_with_inconsistent_adjusted_basis_blocks() -> None:
    action=ConfirmedCorporateAction("AAA",dt.date(2026,7,2),"FORWARD_SPLIT",4.0)
    with pytest.raises(calc_index.IndexValidationError, match="corporate_action_adjusted_basis_unresolved"):
        calc_index.validate_price_return_sanity(
            prev_date=dt.date(2026,7,1),trade_date=dt.date(2026,7,2),tickers=["AAA"],
            prices_prev={"AAA":400.0},prices_now={"AAA":100.0},max_abs_return=0.20,
            action_lookup=lambda *_: action,
        )
