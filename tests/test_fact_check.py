from tools.research_generator.fact_check import run_fact_check


def test_fact_check_blocks_large_delta():
    bundle = {
        "metrics": {
            "core": {"weights_raw": [0.04, 0.04], "weights_sum": 0.08},
            "sector_exposure": {"core_weighted_latest": {"Tech": 1.0}, "coverage_count_latest": {"Tech": 100.0}},
            "top_movers": {
                "incumbent_weight": [
                    {"ticker": "AAA", "delta_weight": 1.0, "weight_prev": 0.04}
                ]
            },
        },
        "docx_tables": [],
    }
    result = run_fact_check(bundle)
    assert "delta_weight_pp_out_of_bounds" in result.critical
