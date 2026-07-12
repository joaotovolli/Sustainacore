import pytest
from app.index_engine.index_integrity import contribution_residual,market_value_residual,rebalance_bridge_residual

def test_contribution_sum_reconciles():
    assert contribution_residual(.03,{'A':.01,'B':.02})==pytest.approx(0)

def test_market_value_identity():
    assert market_value_residual(shares=4,price=25,market_value=100)==pytest.approx(0)

def test_rebalance_bridge_reconciles():
    assert rebalance_bridge_residual(previous_level=1000,divisor=1,shares={'A':5,'B':2.5},prices={'A':100,'B':200})==pytest.approx(0)

def test_rebalance_bridge_requires_all_prices():
    with pytest.raises(ValueError,match='missing_prices'):
        rebalance_bridge_residual(previous_level=1000,divisor=1,shares={'A':1},prices={})
