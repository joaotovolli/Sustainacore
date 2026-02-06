from __future__ import annotations

from app.retrieval.quality_guards import (
    infer_source_type_filters,
    is_greeting_or_thanks,
    is_low_information,
    should_abstain,
)


def test_greeting_detection() -> None:
    assert is_greeting_or_thanks("hello")
    assert is_greeting_or_thanks("Hi")
    assert is_greeting_or_thanks("good morning")
    assert is_greeting_or_thanks("thanks")
    assert is_greeting_or_thanks("thank you")
    assert not is_greeting_or_thanks("What is the status of the EU AI Act?")


def test_low_information_detection() -> None:
    assert is_low_information("")
    assert is_low_information("??")
    assert is_low_information("asdasdasd")  # vowel-less gibberish heuristic
    assert not is_low_information("Tell me about Apple")
    assert not is_low_information("EU AI Act status")


def test_infer_source_type_filters() -> None:
    assert infer_source_type_filters("EU AI Act regulation status") == ["regulatory"]
    assert infer_source_type_filters("latest news") == ["news_release"]
    assert infer_source_type_filters("index performance") == ["performance"]
    assert infer_source_type_filters("Tell me about Apple") == ["company_profile"]


def test_should_abstain_with_low_score() -> None:
    decision = should_abstain(
        "Tell me about asdasdasd",
        [
            {
                "title": "AI Regulation — Montenegro",
                "source_url": "https://sustainacore.org/ai-regulation/",
                "chunk_text": "Jurisdiction: Montenegro ...",
                "score": 0.05,
            }
        ],
    )
    assert decision.abstain
    assert decision.reason == "best_score_too_low"


def test_should_abstain_with_no_overlap_and_mediocre_score() -> None:
    decision = should_abstain(
        "Tell me about Apple",
        [
            {
                "title": "AI Regulation — Montenegro",
                "source_url": "https://sustainacore.org/ai-regulation/",
                "chunk_text": "Jurisdiction: Montenegro ...",
                "score": 0.3,
            }
        ],
    )
    assert decision.abstain
    assert decision.reason == "low_overlap_mediocre_score"


def test_should_not_abstain_with_overlap() -> None:
    decision = should_abstain(
        "Tell me about Apple",
        [
            {
                "title": "Apple (AAPL) | Tech100",
                "source_url": "https://sustainacore.org/tech100/company/AAPL/",
                "chunk_text": "Apple AAPL Information Technology ...",
                "score": 0.3,
            }
        ],
    )
    assert not decision.abstain

