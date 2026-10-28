from __future__ import annotations

import re
from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "docs" / "case-study" / "index.html"


class PageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.images: list[dict[str, str | None]] = []
        self.in_results_table = False
        self.results_table_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        if tag == "img":
            self.images.append(attributes)
        if tag == "table" and attributes.get("id") == "results-table":
            self.in_results_table = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "table" and self.in_results_table:
            self.in_results_table = False

    def handle_data(self, data: str) -> None:
        if self.in_results_table:
            self.results_table_text.append(data)


def test_case_study_images_exist_and_have_alt_text() -> None:
    parser = PageParser()
    parser.feed(PAGE.read_text(encoding="utf-8"))
    assert parser.images
    for image in parser.images:
        assert image.get("alt") is not None
        assert image["alt"].strip()
        assert image.get("src")
        assert (PAGE.parent / str(image["src"])).is_file()


def test_results_figures_are_recorded_in_findings() -> None:
    parser = PageParser()
    parser.feed(PAGE.read_text(encoding="utf-8"))
    findings = (ROOT / "FINDINGS.md").read_text(encoding="utf-8")
    table_text = " ".join(parser.results_table_text)
    figures = re.findall(r"\b\d[\d,]*(?:\.\d+)?\b", table_text)
    assert figures
    assert all(figure in findings for figure in figures)


def test_case_study_is_firm_neutral() -> None:
    page = PAGE.read_text(encoding="utf-8")
    tokens = ["gold" + "man", "morgan " + "stanley", "jp" + "morgan", "c" + "iti",
              "h" + "sbc", "bar" + "clays", "black" + "rock", "black" + "stone",
              "delo" + "itte", "p" + "wc", "inter" + "view"]
    pattern = re.compile("(?i)(?:" + "|".join(r"\b" + re.escape(token) + r"\b" for token in tokens) + ")")
    assert pattern.search(page) is None


def test_new_case_study_files_have_no_utf8_bom() -> None:
    new_files = [PAGE, PAGE.parent / "img" / "end_to_end.png",
                 PAGE.parent / "img" / "snowflake_scd2_query.png",
                 PAGE.parent / "img" / "dbt_lineage.png", ROOT / "tests" / "test_case_study_page.py"]
    for path in new_files:
        assert not path.read_bytes().startswith(b"\xef\xbb\xbf"), path
