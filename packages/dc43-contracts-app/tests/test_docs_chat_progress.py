from dc43_contracts_app import docs_chat


def test_build_progress_summary_renders_details_block():
    summary = docs_chat._build_progress_summary(["first", "second"])  # type: ignore[attr-defined]

    assert summary.startswith("**Processing log**"), summary
    assert "<details><summary>Expand processing steps</summary>" in summary
    assert "- first" in summary
    assert "- second" in summary
