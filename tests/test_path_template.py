"""Tests for path template system."""

from datetime import datetime

import pytest

from eml.layouts.path_template import (
    PathTemplate,
    MessageVars,
    PRESETS,
    LEGACY_PRESETS,
    resolve_preset,
    sanitize_for_path,
    content_hash,
)


class TestSanitizeForPath:
    def test_basic(self):
        assert sanitize_for_path("Hello World") == "hello_world"

    def test_punctuation(self):
        assert sanitize_for_path("Re: Meeting (notes)") == "meeting_notes"

    def test_removes_prefixes(self):
        assert sanitize_for_path("Re: Fwd: Test") == "test"
        assert sanitize_for_path("FW: RE: Subject") == "subject"

    def test_truncation(self):
        long = "a" * 50
        assert len(sanitize_for_path(long, max_len=20)) == 20

    def test_empty(self):
        assert sanitize_for_path("") == "_"
        assert sanitize_for_path("   ") == "_"

    def test_non_ascii(self):
        assert sanitize_for_path("Café résumé") == "caf_r_sum"

    def test_collapses_underscores(self):
        assert sanitize_for_path("a   b   c") == "a_b_c"
        assert sanitize_for_path("a---b___c") == "a_b_c"


class TestContentHash:
    def test_deterministic(self):
        data = b"Hello, World!"
        h1 = content_hash(data)
        h2 = content_hash(data)
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex

    def test_different_content(self):
        h1 = content_hash(b"Hello")
        h2 = content_hash(b"World")
        assert h1 != h2


class TestResolvePreset:
    def test_preset_names(self):
        assert resolve_preset("default") == PRESETS["default"]
        assert resolve_preset("flat") == PRESETS["flat"]
        assert resolve_preset("daily") == PRESETS["daily"]

    def test_legacy_names(self):
        # Legacy names should resolve to templates
        assert "$" in resolve_preset("tree:month")
        assert "$" in resolve_preset("tree:flat")
        assert "$" in resolve_preset("tree:day")

    def test_raw_template(self):
        template = "$folder/$yyyy/${sha8}.eml"
        assert resolve_preset(template) == template


class TestMessageVars:
    def test_to_dict(self):
        vars = MessageVars(
            folder="INBOX",
            raw=b"From: test@example.com\r\nSubject: Test\r\n\r\nBody",
            date=datetime(2024, 3, 15, 14, 30, 45),
            subject="Re: Meeting Notes",
            from_addr="john.smith@example.com",
            uid=12345,
        )
        d = vars.to_dict()

        # Folder
        assert d["folder"] == "INBOX"

        # Date components
        assert d["yyyy"] == "2024"
        assert d["yy"] == "24"
        assert d["mm"] == "03"
        assert d["dd"] == "15"
        assert d["hh"] == "14"
        assert d["MM"] == "30"
        assert d["ss"] == "45"
        assert d["hhmm"] == "1430"
        assert d["hhmmss"] == "143045"

        # Hash variants
        assert len(d["sha"]) == 64
        assert len(d["sha8"]) == 8
        assert len(d["sha16"]) == 16
        assert d["sha8"] == d["sha"][:8]

        # Subject variants
        assert d["subj"] == "meeting_notes"
        assert len(d["subj10"]) <= 10
        assert len(d["subj20"]) <= 20

        # From variants
        assert "john" in d["from"] or "smith" in d["from"]

        # UID
        assert d["uid"] == "12345"

    def test_missing_date_uses_now(self):
        vars = MessageVars(
            folder="INBOX",
            raw=b"test",
            date=None,
        )
        d = vars.to_dict()
        # Should have date values from current time
        assert d["yyyy"].isdigit()
        assert len(d["yyyy"]) == 4


class TestPathTemplate:
    def test_preset(self):
        pt = PathTemplate("default")
        assert pt.original == "default"
        assert "$folder" in pt.template_str

    def test_legacy_preset(self):
        pt = PathTemplate("tree:month")
        assert pt.original == "tree:month"
        assert "$folder" in pt.template_str

    def test_raw_template(self):
        template = "$folder/$yyyy/${sha8}.eml"
        pt = PathTemplate(template)
        assert pt.original == template
        assert pt.template_str == template

    def test_variables(self):
        pt = PathTemplate("$folder/$yyyy/$mm/${sha8}.eml")
        vars = pt.variables
        assert "folder" in vars
        assert "yyyy" in vars
        assert "mm" in vars
        assert "sha8" in vars

    def test_render(self):
        pt = PathTemplate("$folder/$yyyy/$mm/${sha8}_${subj}.eml")
        vars = MessageVars(
            folder="INBOX",
            raw=b"test message content",
            date=datetime(2024, 3, 15),
            subject="Hello World",
        )
        path = pt.render(vars)

        assert path.startswith("INBOX/2024/03/")
        assert path.endswith(".eml")
        assert "_hello_world" in path

    def test_render_message(self):
        pt = PathTemplate("$folder/$yyyy/${sha8}.eml")
        path = pt.render_message(
            folder="Sent",
            raw=b"test",
            date=datetime(2024, 6, 1),
        )
        assert path.startswith("Sent/2024/")
        assert path.endswith(".eml")

    def test_repr(self):
        pt = PathTemplate("default")
        repr_str = repr(pt)
        assert "default" in repr_str
        assert "PathTemplate" in repr_str


class TestPresets:
    def test_all_presets_contain_required_vars(self):
        """All presets should have folder and sha for proper storage."""
        for name, template in PRESETS.items():
            assert "$folder" in template, f"{name} missing $folder"
            assert "$sha" in template or "${sha" in template, f"{name} missing sha"

    def test_all_presets_end_with_eml(self):
        for name, template in PRESETS.items():
            assert template.endswith(".eml"), f"{name} doesn't end with .eml"


class TestIntegration:
    def test_full_workflow(self):
        """Test complete path generation workflow."""
        # Create template
        pt = PathTemplate("$folder/$yyyy/$mm/$dd/${hhmm}_${sha8}_${subj20}.eml")

        # Create message vars
        raw = b"From: alice@example.com\r\nSubject: Project Update\r\n\r\nDetails..."
        vars = MessageVars(
            folder="Work/Projects",
            raw=raw,
            date=datetime(2024, 12, 25, 9, 30),
            subject="Re: Project Update - Q4 Goals",
            from_addr="alice@example.com",
        )

        # Render path
        path = pt.render(vars)

        # Verify structure
        parts = path.split("/")
        assert parts[0] == "Work"
        assert parts[1] == "Projects"
        assert parts[2] == "2024"
        assert parts[3] == "12"
        assert parts[4] == "25"
        assert parts[5].startswith("0930_")
        assert parts[5].endswith(".eml")
        assert "project_update" in parts[5]
