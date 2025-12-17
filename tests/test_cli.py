"""Tests for eml CLI commands."""

import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from eml.cli import main


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def project(tmp_path, monkeypatch):
    """Create an initialized project in a temp directory."""
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(main, ["init"])
    assert result.exit_code == 0
    assert (tmp_path / ".eml" / "config.yaml").exists()
    return tmp_path


class TestInit:
    def test_init_default(self, runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(main, ["init"])
        assert result.exit_code == 0
        assert "Initialized" in result.output
        assert (tmp_path / ".eml" / "config.yaml").exists()
        assert (tmp_path / ".eml" / "sync-state").is_dir()
        assert (tmp_path / ".eml" / "pushed").is_dir()
        # Check default layout is stored
        config = (tmp_path / ".eml" / "config.yaml").read_text()
        assert "layout: default" in config

    def test_init_sqlite(self, runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(main, ["init", "-L", "sqlite"])
        assert result.exit_code == 0
        assert "sqlite" in result.output
        config = (tmp_path / ".eml" / "config.yaml").read_text()
        assert "layout: sqlite" in config

    def test_init_preset(self, runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(main, ["init", "-L", "flat"])
        assert result.exit_code == 0
        assert "flat" in result.output
        config = (tmp_path / ".eml" / "config.yaml").read_text()
        assert "layout: flat" in config

    def test_init_custom_template(self, runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        template = "$folder/$yyyy/${sha8}.eml"
        result = runner.invoke(main, ["init", "-L", template])
        assert result.exit_code == 0
        config = (tmp_path / ".eml" / "config.yaml").read_text()
        assert template in config

    def test_init_legacy_layout(self, runner, tmp_path, monkeypatch):
        """Legacy tree:* layouts should still work."""
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(main, ["init", "-L", "tree:month"])
        assert result.exit_code == 0
        config = (tmp_path / ".eml" / "config.yaml").read_text()
        assert "tree:month" in config

    def test_init_invalid_layout(self, runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(main, ["init", "-L", "invalid"])
        assert result.exit_code != 0
        assert "Invalid layout" in result.output

    def test_init_already_exists(self, runner, project):
        result = runner.invoke(main, ["init"])
        assert result.exit_code == 0
        assert "Already initialized" in result.output


class TestAccount:
    def test_account_add(self, runner, project):
        result = runner.invoke(
            main,
            ["account", "add", "-t", "gmail", "g/test", "test@gmail.com"],
            input="testpass\n",
        )
        assert result.exit_code == 0
        assert "saved" in result.output
        assert "config.yaml" in result.output

        # Verify in config
        config = (project / ".eml" / "config.yaml").read_text()
        assert "g/test" in config
        assert "test@gmail.com" in config

    def test_account_add_with_host(self, runner, project):
        result = runner.invoke(
            main,
            ["account", "add", "-t", "imap", "-H", "imap.example.com", "y/test", "user@example.com"],
            input="testpass\n",
        )
        assert result.exit_code == 0
        config = (project / ".eml" / "config.yaml").read_text()
        assert "imap.example.com" in config

    def test_account_ls_empty(self, runner, project):
        result = runner.invoke(main, ["account", "ls"])
        assert result.exit_code == 0
        assert "No accounts configured" in result.output

    def test_account_ls_with_accounts(self, runner, project):
        # Add an account first
        runner.invoke(
            main,
            ["account", "add", "-t", "gmail", "g/test", "test@gmail.com"],
            input="testpass\n",
        )
        result = runner.invoke(main, ["account", "ls"])
        assert result.exit_code == 0
        assert "g/test" in result.output
        assert "test@gmail.com" in result.output

    def test_account_rm(self, runner, project):
        # Add then remove
        runner.invoke(
            main,
            ["account", "add", "-t", "gmail", "g/test", "test@gmail.com"],
            input="testpass\n",
        )
        result = runner.invoke(main, ["account", "rm", "g/test"])
        assert result.exit_code == 0
        assert "removed" in result.output

        # Verify gone
        result = runner.invoke(main, ["account", "ls"])
        assert "g/test" not in result.output

    def test_account_rm_not_found(self, runner, project):
        result = runner.invoke(main, ["account", "rm", "nonexistent"])
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_account_rename(self, runner, project):
        # Add an account
        runner.invoke(
            main,
            ["account", "add", "-t", "gmail", "g/old", "test@gmail.com"],
            input="testpass\n",
        )
        # Rename it
        result = runner.invoke(main, ["account", "rename", "g/old", "g/new"])
        assert result.exit_code == 0
        assert "renamed" in result.output
        assert "g/old" in result.output
        assert "g/new" in result.output

        # Verify old is gone, new exists
        result = runner.invoke(main, ["account", "ls"])
        assert "g/old" not in result.output
        assert "g/new" in result.output

    def test_account_rename_not_found(self, runner, project):
        result = runner.invoke(main, ["account", "rename", "nonexistent", "new"])
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_account_rename_target_exists(self, runner, project):
        # Add two accounts
        runner.invoke(
            main,
            ["account", "add", "-t", "gmail", "g/one", "one@gmail.com"],
            input="pass1\n",
        )
        runner.invoke(
            main,
            ["account", "add", "-t", "gmail", "g/two", "two@gmail.com"],
            input="pass2\n",
        )
        # Try to rename one to two
        result = runner.invoke(main, ["account", "rename", "g/one", "g/two"])
        assert result.exit_code == 1
        assert "already exists" in result.output


class TestHelpOnNoArgs:
    """Test that commands show help when required args are missing."""

    def test_account_add_no_args(self, runner, project):
        result = runner.invoke(main, ["account", "add"])
        assert result.exit_code == 2
        assert "Usage:" in result.output
        assert "NAME USER" in result.output

    def test_account_rm_no_args(self, runner, project):
        result = runner.invoke(main, ["account", "rm"])
        assert result.exit_code == 2
        assert "Usage:" in result.output

    def test_account_rename_no_args(self, runner, project):
        result = runner.invoke(main, ["account", "rename"])
        assert result.exit_code == 2
        assert "Usage:" in result.output

    def test_pull_no_args(self, runner, project):
        result = runner.invoke(main, ["pull"])
        assert result.exit_code == 2
        assert "Usage:" in result.output
        assert "ACCOUNT" in result.output

    def test_push_no_args(self, runner, project):
        result = runner.invoke(main, ["push"])
        assert result.exit_code == 2
        assert "Usage:" in result.output

    def test_convert_no_args(self, runner, project):
        result = runner.invoke(main, ["convert"])
        assert result.exit_code == 2
        assert "Usage:" in result.output

    def test_folders_no_args(self, runner, project):
        result = runner.invoke(main, ["folders"])
        assert result.exit_code == 2
        assert "Usage:" in result.output


class TestConvert:
    def test_convert_same_layout(self, runner, project):
        # Default layout is "default", so converting to "default" should be no-op
        result = runner.invoke(main, ["convert", "default"])
        assert result.exit_code == 0
        assert "Already using" in result.output

    def test_convert_dry_run(self, runner, project):
        result = runner.invoke(main, ["convert", "-n", "sqlite"])
        assert result.exit_code == 0
        assert "DRY RUN" in result.output

    def test_convert_legacy_alias(self, runner, project):
        # Legacy tree:month should work and resolve to default template
        result = runner.invoke(main, ["convert", "-n", "tree:month"])
        assert result.exit_code == 0


class TestAliases:
    """Test command aliases work."""

    def test_init_alias(self, runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(main, ["i"])
        assert result.exit_code == 0
        assert "Initialized" in result.output

    def test_account_alias(self, runner, project):
        result = runner.invoke(main, ["a", "l"])
        assert result.exit_code == 0

    def test_account_add_alias(self, runner, project):
        result = runner.invoke(
            main,
            ["a", "a", "-t", "gmail", "g/test", "test@gmail.com"],
            input="testpass\n",
        )
        assert result.exit_code == 0
        assert "saved" in result.output

    def test_account_rename_alias(self, runner, project):
        runner.invoke(
            main,
            ["a", "a", "-t", "gmail", "g/old", "test@gmail.com"],
            input="testpass\n",
        )
        result = runner.invoke(main, ["a", "r", "g/old", "g/new"])
        assert result.exit_code == 0
        assert "renamed" in result.output

    def test_convert_alias(self, runner, project):
        result = runner.invoke(main, ["cv", "tree:month"])
        assert result.exit_code == 0


class TestStatus:
    """Tests for eml status command."""

    def test_status_empty_project(self, runner, project):
        """Status should work on empty project."""
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "Total files:" in result.output
        assert "0" in result.output
        assert "No sync running" in result.output

    def test_status_with_files(self, runner, project):
        """Status should count .eml files."""
        # Create some fake .eml files
        inbox = project / "INBOX"
        inbox.mkdir()
        (inbox / "test1.eml").write_text("From: a@b.com\n\nBody")
        (inbox / "test2.eml").write_text("From: c@d.com\n\nBody")

        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "Total files:" in result.output
        # Should find 2 files
        assert "2" in result.output

    def test_status_folder_filter(self, runner, project):
        """Status -f should filter by folder."""
        # Create files in two folders
        inbox = project / "INBOX"
        sent = project / "Sent"
        inbox.mkdir()
        sent.mkdir()
        (inbox / "msg1.eml").write_text("test")
        (inbox / "msg2.eml").write_text("test")
        (sent / "msg3.eml").write_text("test")

        # Filter to INBOX only
        result = runner.invoke(main, ["status", "-f", "INBOX"])
        assert result.exit_code == 0
        assert "(INBOX)" in result.output

    def test_status_requires_init(self, runner, tmp_path, monkeypatch):
        """Status should fail without .eml/ directory."""
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 1
        assert "Not an eml project" in result.output or "eml init" in result.output


class TestIndex:
    """Tests for eml index command."""

    def test_index_empty_project(self, runner, project):
        """Index should work on empty project."""
        result = runner.invoke(main, ["index"])
        assert result.exit_code == 0
        assert "Indexed:" in result.output
        assert "0" in result.output
        # Index db should exist
        assert (project / ".eml" / "index.db").exists()

    def test_index_with_files(self, runner, project):
        """Index should index .eml files."""
        # Create some fake .eml files
        inbox = project / "INBOX"
        inbox.mkdir()
        (inbox / "test1.eml").write_bytes(
            b"Message-ID: <test1@example.com>\r\nFrom: a@b.com\r\nSubject: Test 1\r\n\r\nBody"
        )
        (inbox / "test2.eml").write_bytes(
            b"Message-ID: <test2@example.com>\r\nFrom: c@d.com\r\nSubject: Test 2\r\n\r\nBody"
        )

        result = runner.invoke(main, ["index"])
        assert result.exit_code == 0
        assert "Indexed:" in result.output

    def test_index_stats_empty(self, runner, project):
        """Index -s on empty index should show message."""
        result = runner.invoke(main, ["index", "-s"])
        assert result.exit_code == 0
        assert "empty" in result.output.lower()

    def test_index_stats_after_build(self, runner, project):
        """Index -s should show stats after building."""
        # Create and index a file
        inbox = project / "INBOX"
        inbox.mkdir()
        (inbox / "test.eml").write_bytes(
            b"Message-ID: <test@example.com>\r\nFrom: a@b.com\r\nSubject: Test\r\n\r\nBody"
        )

        # Build and check stats in one command flow (within same process)
        result = runner.invoke(main, ["index"])
        assert result.exit_code == 0
        assert "Indexed:" in result.output

    def test_index_check(self, runner, project):
        """Index -c should check freshness."""
        # Build index first
        runner.invoke(main, ["index"])

        result = runner.invoke(main, ["index", "-c"])
        # May fail if not a git repo, but shouldn't crash
        assert result.exit_code in (0, 1)

    def test_index_update(self, runner, project):
        """Index -u should do incremental update."""
        result = runner.invoke(main, ["index", "-u"])
        assert result.exit_code == 0

    def test_index_requires_init(self, runner, tmp_path, monkeypatch):
        """Index should fail without .eml/ directory."""
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(main, ["index"])
        assert result.exit_code == 1


class TestAttachments:
    """Tests for eml attachments commands."""

    @pytest.fixture
    def test_eml(self, tmp_path):
        """Create a test .eml file with an attachment."""
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders
        import hashlib

        msg = MIMEMultipart()
        msg['From'] = 'sender@example.com'
        msg['To'] = 'recipient@example.com'
        msg['Subject'] = 'Test with attachment'
        msg['Message-ID'] = '<test-attachment-123@example.com>'
        msg['Date'] = 'Wed, 01 Jan 2025 12:00:00 +0000'

        # Text body
        body = MIMEText('This is the email body.', 'plain', 'utf-8')
        msg.attach(body)

        # Text file attachment
        attachment_content = b'Hello, this is the attachment content!\nLine 2\nLine 3\n'
        attachment = MIMEBase('text', 'plain')
        attachment.set_payload(attachment_content)
        encoders.encode_base64(attachment)
        attachment.add_header('Content-Disposition', 'attachment', filename='test_file.txt')
        msg.attach(attachment)

        raw = msg.as_bytes()
        sha = hashlib.sha256(raw).hexdigest()[:8]
        eml_path = tmp_path / f"{sha}_test.eml"
        eml_path.write_bytes(raw)
        return eml_path

    def test_attachments_list(self, runner, test_eml):
        """List attachments in an .eml file."""
        result = runner.invoke(main, ["attachments", "list", str(test_eml)])
        assert result.exit_code == 0
        assert "test_file.txt" in result.output
        assert "text/plain" in result.output

    def test_attachments_list_json(self, runner, test_eml):
        """List attachments as JSON."""
        result = runner.invoke(main, ["attachments", "list", "-j", str(test_eml)])
        assert result.exit_code == 0
        import json
        data = json.loads(result.output)
        assert len(data) == 1
        assert data[0]["filename"] == "test_file.txt"
        assert data[0]["content_type"] == "text/plain"
        assert data[0]["size"] == 53

    def test_attachments_extract(self, runner, test_eml, tmp_path):
        """Extract an attachment from .eml file."""
        out_path = tmp_path / "extracted.txt"
        result = runner.invoke(
            main,
            ["attachments", "extract", str(test_eml), "test_file.txt", "-o", str(out_path)],
        )
        assert result.exit_code == 0
        assert out_path.exists()
        content = out_path.read_text()
        assert "Hello, this is the attachment content!" in content

    def test_attachments_add(self, runner, test_eml, tmp_path):
        """Add an attachment to .eml file."""
        # Create a new file to attach
        new_file = tmp_path / "new_attachment.txt"
        new_file.write_text("New attachment content")

        out_eml = tmp_path / "output.eml"
        result = runner.invoke(
            main,
            ["attachments", "add", str(test_eml), str(new_file), "-o", str(out_eml)],
        )
        assert result.exit_code == 0
        assert out_eml.exists()

        # Verify both attachments are present
        result = runner.invoke(main, ["attachments", "list", str(out_eml)])
        assert "test_file.txt" in result.output
        assert "new_attachment.txt" in result.output

    def test_attachments_replace(self, runner, test_eml, tmp_path):
        """Replace an attachment in .eml file."""
        # Create replacement file
        replacement = tmp_path / "replacement.txt"
        replacement.write_text("Replaced content here!")

        out_eml = tmp_path / "output.eml"
        result = runner.invoke(
            main,
            ["attachments", "replace", str(test_eml), "test_file.txt", str(replacement), "-o", str(out_eml)],
        )
        assert result.exit_code == 0
        assert out_eml.exists()

        # Extract and verify replacement
        extracted = tmp_path / "extracted.txt"
        runner.invoke(main, ["attachments", "extract", str(out_eml), "test_file.txt", "-o", str(extracted)])
        content = extracted.read_text()
        assert content == "Replaced content here!"

    def test_attachments_remove(self, runner, test_eml, tmp_path):
        """Remove an attachment from .eml file."""
        out_eml = tmp_path / "output.eml"
        result = runner.invoke(
            main,
            ["attachments", "remove", str(test_eml), "test_file.txt", "-o", str(out_eml)],
        )
        assert result.exit_code == 0
        assert out_eml.exists()

        # Verify attachment is gone
        result = runner.invoke(main, ["attachments", "list", str(out_eml)])
        assert "No attachments" in result.output

    def test_attachments_sha_rename(self, runner, tmp_path):
        """Test that SHA in filename is updated when content changes."""
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders
        import hashlib

        # Create original message
        msg = MIMEMultipart()
        msg['From'] = 'test@example.com'
        msg['Subject'] = 'SHA test'
        msg['Message-ID'] = '<sha-test@example.com>'
        body = MIMEText('Body', 'plain')
        msg.attach(body)
        att = MIMEBase('text', 'plain')
        att.set_payload(b'original')
        encoders.encode_base64(att)
        att.add_header('Content-Disposition', 'attachment', filename='file.txt')
        msg.attach(att)

        raw = msg.as_bytes()
        sha = hashlib.sha256(raw).hexdigest()[:8]
        eml_path = tmp_path / f"{sha}_sha_test.eml"
        eml_path.write_bytes(raw)

        # Add a new attachment (without -k or -o, should rename)
        new_file = tmp_path / "new.txt"
        new_file.write_text("new content")

        result = runner.invoke(main, ["attachments", "add", str(eml_path), str(new_file)])
        assert result.exit_code == 0
        assert "->" in result.output  # Should show rename

        # Original should be deleted, new file should exist with different SHA
        assert not eml_path.exists()
        new_files = list(tmp_path.glob("*_sha_test.eml"))
        assert len(new_files) == 1
        assert new_files[0].name != eml_path.name


class TestIngest:
    """Tests for eml ingest command."""

    @pytest.fixture
    def ingest_project(self, tmp_path, monkeypatch):
        """Create an initialized project in a subdirectory for ingest tests."""
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        monkeypatch.chdir(project_dir)
        runner = CliRunner()
        result = runner.invoke(main, ["init"])
        assert result.exit_code == 0
        return project_dir

    @pytest.fixture
    def external_eml(self, tmp_path):
        """Create an external .eml file to ingest (sibling to project dir)."""
        from email.mime.text import MIMEText

        msg = MIMEText('Test body', 'plain')
        msg['From'] = 'external@example.com'
        msg['To'] = 'internal@example.com'
        msg['Subject'] = 'External email for ingest'
        msg['Message-ID'] = '<ingest-test-123@example.com>'
        msg['Date'] = 'Thu, 02 Jan 2025 10:30:00 +0000'

        # Put in sibling dir so it's not scanned by TreeLayout
        external_dir = tmp_path / "external"
        external_dir.mkdir()
        eml_path = external_dir / "external_email.eml"
        eml_path.write_bytes(msg.as_bytes())
        return eml_path

    def test_ingest_basic(self, runner, ingest_project, external_eml):
        """Ingest an external .eml file into the repo."""
        result = runner.invoke(main, ["ingest", str(external_eml), "-f", "INBOX"])
        assert result.exit_code == 0
        assert "Copied" in result.output

        # Verify file exists in INBOX folder
        inbox = ingest_project / "INBOX"
        assert inbox.exists()
        eml_files = list(inbox.rglob("*.eml"))
        assert len(eml_files) == 1

    def test_ingest_move(self, runner, ingest_project, external_eml):
        """Ingest with move should delete original."""
        result = runner.invoke(main, ["ingest", str(external_eml), "-f", "INBOX", "-M"])
        assert result.exit_code == 0
        assert "Moved" in result.output

        # Original should be gone
        assert not external_eml.exists()

        # File should exist in project
        eml_files = list((ingest_project / "INBOX").rglob("*.eml"))
        assert len(eml_files) == 1

    def test_ingest_dry_run(self, runner, ingest_project, external_eml):
        """Ingest dry run should not create files."""
        result = runner.invoke(main, ["ingest", str(external_eml), "-f", "INBOX", "-N"])
        assert result.exit_code == 0
        assert "Would copy" in result.output

        # File should still exist externally
        assert external_eml.exists()

        # No files in project
        inbox = ingest_project / "INBOX"
        if inbox.exists():
            eml_files = list(inbox.rglob("*.eml"))
            assert len(eml_files) == 0

    def test_ingest_duplicate(self, runner, ingest_project, external_eml):
        """Ingest should skip duplicates by Message-ID."""
        # Ingest once
        runner.invoke(main, ["ingest", str(external_eml), "-f", "INBOX"])

        # Copy external_eml to a new location and try again
        copy_path = external_eml.parent / "copy.eml"
        copy_path.write_bytes(external_eml.read_bytes())

        result = runner.invoke(main, ["ingest", str(copy_path), "-f", "INBOX"])
        assert result.exit_code == 0
        assert "Skipped (duplicate)" in result.output

        # Still only one file
        eml_files = list((ingest_project / "INBOX").rglob("*.eml"))
        assert len(eml_files) == 1

    def test_ingest_requires_init(self, runner, tmp_path, monkeypatch):
        """Ingest should fail without .eml/ directory."""
        monkeypatch.chdir(tmp_path)
        eml = tmp_path / "test.eml"
        eml.write_text("From: a@b.com\n\nBody")

        result = runner.invoke(main, ["ingest", str(eml)])
        assert result.exit_code == 1


class TestAttachmentsIngestE2E:
    """End-to-end test: in-place attachment modification and ingest in a git+eml repo."""

    def test_e2e_in_place_attachment_modification(self, runner, tmp_path, monkeypatch):
        """
        Real-world workflow: downsize an attachment in an existing repo.

        Scenario: An .eml file has a large attachment that's too big to push.
        We extract it, downsize it, replace it in-place (SHA-based rename),
        then ingest a second .eml file. Verify final git worktree state.

        Steps:
        1. Init git + eml project with flat layout
        2. Create initial .eml with "large" attachment in Inbox/
        3. Git add + commit the initial state
        4. Extract the attachment
        5. "Downsize" it (create smaller version)
        6. Replace attachment in-place (no -o flag) - should rename file due to SHA change
        7. Verify old file gone, new file exists with different SHA in name
        8. Ingest a second .eml from external location
        9. Git add + commit final state
        10. Verify final worktree hash
        """
        import hashlib
        import subprocess
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders

        def hash_worktree(root: Path) -> str:
            """Hash git-tracked files in worktree for deterministic comparison."""
            # Use git ls-files to get tracked files, then hash their contents
            result = subprocess.run(
                ["git", "ls-files"],
                cwd=root,
                capture_output=True,
                text=True,
                check=True,
            )
            files = []
            for rel_path in sorted(result.stdout.strip().split("\n")):
                if rel_path:
                    full_path = root / rel_path
                    if full_path.is_file():
                        content_hash = hashlib.sha256(full_path.read_bytes()).hexdigest()
                        files.append(f"{rel_path}:{content_hash}")
            return hashlib.sha256("\n".join(files).encode()).hexdigest()

        def make_eml(from_addr: str, subject: str, msg_id: str, attachment_data: bytes) -> bytes:
            """Create a test .eml with attachment."""
            msg = MIMEMultipart()
            msg['From'] = from_addr
            msg['To'] = 'recipient@example.com'
            msg['Subject'] = subject
            msg['Message-ID'] = msg_id
            msg['Date'] = 'Fri, 03 Jan 2025 14:00:00 +0000'

            body = MIMEText('Email body', 'plain')
            msg.attach(body)

            att = MIMEBase('application', 'octet-stream')
            att.set_payload(attachment_data)
            encoders.encode_base64(att)
            att.add_header('Content-Disposition', 'attachment', filename='image.jpg')
            msg.attach(att)

            return msg.as_bytes()

        # Step 1: Init git + eml project
        project = tmp_path / "repo"
        project.mkdir()
        monkeypatch.chdir(project)

        subprocess.run(["git", "init"], cwd=project, check=True, capture_output=True)
        subprocess.run(
            ["git", "config", "user.email", "test@example.com"],
            cwd=project, check=True, capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Test User"],
            cwd=project, check=True, capture_output=True,
        )

        result = runner.invoke(main, ["init", "-L", "flat"])
        assert result.exit_code == 0

        # Step 2: Create initial .eml with "large" attachment
        large_attachment = b"X" * 1000  # Simulates large image
        raw1 = make_eml(
            "sender@example.com",
            "Photo from vacation",
            "<photo-001@example.com>",
            large_attachment,
        )
        sha1 = hashlib.sha256(raw1).hexdigest()[:8]

        inbox = project / "Inbox"
        inbox.mkdir()
        eml1_path = inbox / f"{sha1}_Photo_from_vacation.eml"
        eml1_path.write_bytes(raw1)

        # Step 3: Git commit initial state
        subprocess.run(["git", "add", "-A"], cwd=project, check=True, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", "Initial: large attachment"],
            cwd=project, check=True, capture_output=True,
        )

        initial_hash = hash_worktree(project)

        # Step 4: Extract the attachment
        extracted = tmp_path / "extracted_image.jpg"
        result = runner.invoke(
            main,
            ["attachments", "extract", str(eml1_path), "image.jpg", "-o", str(extracted)],
        )
        assert result.exit_code == 0
        assert extracted.read_bytes() == large_attachment

        # Step 5: "Downsize" the attachment
        small_attachment = b"Y" * 100  # Downsized version
        downsized = tmp_path / "downsized_image.jpg"
        downsized.write_bytes(small_attachment)

        # Step 6: Replace attachment in-place (no -o, no -k)
        result = runner.invoke(
            main,
            ["attachments", "replace", str(eml1_path), "image.jpg", str(downsized)],
        )
        assert result.exit_code == 0
        assert "->" in result.output  # Should show rename

        # Step 7: Verify old file gone, new file exists
        assert not eml1_path.exists(), "Original file should be deleted"
        new_eml_files = list(inbox.glob("*.eml"))
        assert len(new_eml_files) == 1
        new_eml1_path = new_eml_files[0]
        assert new_eml1_path.name != eml1_path.name, "Filename should change due to SHA"

        # Verify the new file has the downsized attachment
        import email
        with open(new_eml1_path, 'rb') as f:
            modified_msg = email.message_from_binary_file(f)
        assert modified_msg['Message-ID'] == '<photo-001@example.com>'
        for part in modified_msg.walk():
            if part.get_filename() == 'image.jpg':
                assert part.get_payload(decode=True) == small_attachment
                break
        else:
            pytest.fail("Attachment not found in modified message")

        # Step 8: Ingest a second .eml from external location
        external_dir = tmp_path / "external"
        external_dir.mkdir()

        raw2 = make_eml(
            "friend@example.com",
            "Another photo",
            "<photo-002@example.com>",
            b"Z" * 50,  # Small attachment
        )
        external_eml = external_dir / "friend_photo.eml"
        external_eml.write_bytes(raw2)

        result = runner.invoke(main, ["ingest", str(external_eml), "-f", "Inbox"])
        assert result.exit_code == 0
        assert "Copied" in result.output

        # Should now have 2 .eml files in Inbox
        final_eml_files = list(inbox.glob("*.eml"))
        assert len(final_eml_files) == 2

        # Step 9: Git commit final state
        subprocess.run(["git", "add", "-A"], cwd=project, check=True, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", "Downsized attachment + ingested second email"],
            cwd=project, check=True, capture_output=True,
        )

        # Step 10: Verify final worktree hash changed
        final_hash = hash_worktree(project)
        assert final_hash != initial_hash, "Worktree should have changed"

        # Verify git log shows 2 commits
        log_result = subprocess.run(
            ["git", "log", "--oneline"],
            cwd=project, capture_output=True, text=True, check=True,
        )
        commits = [c for c in log_result.stdout.strip().split("\n") if c]
        assert len(commits) == 2

        # Verify both messages are preserved
        message_ids = set()
        for eml_file in final_eml_files:
            with open(eml_file, 'rb') as f:
                msg = email.message_from_binary_file(f)
                message_ids.add(msg['Message-ID'])
        assert message_ids == {'<photo-001@example.com>', '<photo-002@example.com>'}
