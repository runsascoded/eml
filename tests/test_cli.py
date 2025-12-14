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
def v2_project(tmp_path, monkeypatch):
    """Create a V2 project in a temp directory."""
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(main, ["init"])
    assert result.exit_code == 0
    assert (tmp_path / ".eml" / "config.yaml").exists()
    return tmp_path


@pytest.fixture
def v1_project(tmp_path, monkeypatch):
    """Create a V1 project in a temp directory."""
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(main, ["init", "-V"])
    assert result.exit_code == 0
    assert (tmp_path / ".eml" / "msgs.db").exists()
    return tmp_path


class TestInit:
    def test_init_v2_default(self, runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(main, ["init"])
        assert result.exit_code == 0
        assert "Initialized (V2)" in result.output
        assert (tmp_path / ".eml" / "config.yaml").exists()
        assert (tmp_path / ".eml" / "sync-state").is_dir()
        assert (tmp_path / ".eml" / "pushed").is_dir()

    def test_init_v2_sqlite(self, runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(main, ["init", "-L", "sqlite"])
        assert result.exit_code == 0
        assert "sqlite" in result.output
        config = (tmp_path / ".eml" / "config.yaml").read_text()
        assert "layout: sqlite" in config

    def test_init_v1(self, runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(main, ["init", "-V"])
        assert result.exit_code == 0
        assert "Initialized (V1)" in result.output
        assert (tmp_path / ".eml" / "msgs.db").exists()
        assert (tmp_path / ".eml" / "accts.db").exists()

    def test_init_already_exists(self, runner, v2_project):
        result = runner.invoke(main, ["init"])
        assert result.exit_code == 0
        assert "Already initialized" in result.output


class TestAccount:
    def test_account_add_v2(self, runner, v2_project):
        result = runner.invoke(
            main,
            ["account", "add", "-t", "gmail", "g/test", "test@gmail.com"],
            input="testpass\n",
        )
        assert result.exit_code == 0
        assert "saved" in result.output
        assert "config.yaml" in result.output

        # Verify in config
        config = (v2_project / ".eml" / "config.yaml").read_text()
        assert "g/test" in config
        assert "test@gmail.com" in config

    def test_account_add_with_host(self, runner, v2_project):
        result = runner.invoke(
            main,
            ["account", "add", "-t", "imap", "-H", "imap.example.com", "y/test", "user@example.com"],
            input="testpass\n",
        )
        assert result.exit_code == 0
        config = (v2_project / ".eml" / "config.yaml").read_text()
        assert "imap.example.com" in config

    def test_account_ls_empty(self, runner, v2_project):
        result = runner.invoke(main, ["account", "ls"])
        assert result.exit_code == 0
        assert "No accounts configured" in result.output

    def test_account_ls_with_accounts(self, runner, v2_project):
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

    def test_account_rm(self, runner, v2_project):
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

    def test_account_rm_not_found(self, runner, v2_project):
        result = runner.invoke(main, ["account", "rm", "nonexistent"])
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_account_rename(self, runner, v2_project):
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

    def test_account_rename_not_found(self, runner, v2_project):
        result = runner.invoke(main, ["account", "rename", "nonexistent", "new"])
        assert result.exit_code == 1
        assert "not found" in result.output

    def test_account_rename_target_exists(self, runner, v2_project):
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

    def test_account_add_no_args(self, runner, v2_project):
        result = runner.invoke(main, ["account", "add"])
        assert result.exit_code == 2
        assert "Usage:" in result.output
        assert "NAME USER" in result.output

    def test_account_rm_no_args(self, runner, v2_project):
        result = runner.invoke(main, ["account", "rm"])
        assert result.exit_code == 2
        assert "Usage:" in result.output

    def test_account_rename_no_args(self, runner, v2_project):
        result = runner.invoke(main, ["account", "rename"])
        assert result.exit_code == 2
        assert "Usage:" in result.output

    def test_pull_no_args(self, runner, v2_project):
        result = runner.invoke(main, ["pull"])
        assert result.exit_code == 2
        assert "Usage:" in result.output
        assert "ACCOUNT" in result.output

    def test_push_no_args(self, runner, v2_project):
        result = runner.invoke(main, ["push"])
        assert result.exit_code == 2
        assert "Usage:" in result.output

    def test_convert_no_args(self, runner, v2_project):
        result = runner.invoke(main, ["convert"])
        assert result.exit_code == 2
        assert "Usage:" in result.output

    def test_folders_no_args(self, runner, v2_project):
        result = runner.invoke(main, ["folders"])
        assert result.exit_code == 2
        assert "Usage:" in result.output


class TestConvert:
    def test_convert_same_layout(self, runner, v2_project):
        result = runner.invoke(main, ["convert", "tree:month"])
        assert result.exit_code == 0
        assert "Already using" in result.output

    def test_convert_dry_run(self, runner, v2_project):
        result = runner.invoke(main, ["convert", "-n", "sqlite"])
        assert result.exit_code == 0
        assert "DRY RUN" in result.output


class TestAliases:
    """Test command aliases work."""

    def test_init_alias(self, runner, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(main, ["i"])
        assert result.exit_code == 0
        assert "Initialized" in result.output

    def test_account_alias(self, runner, v2_project):
        result = runner.invoke(main, ["a", "l"])
        assert result.exit_code == 0

    def test_account_add_alias(self, runner, v2_project):
        result = runner.invoke(
            main,
            ["a", "a", "-t", "gmail", "g/test", "test@gmail.com"],
            input="testpass\n",
        )
        assert result.exit_code == 0
        assert "saved" in result.output

    def test_account_rename_alias(self, runner, v2_project):
        runner.invoke(
            main,
            ["a", "a", "-t", "gmail", "g/old", "test@gmail.com"],
            input="testpass\n",
        )
        result = runner.invoke(main, ["a", "r", "g/old", "g/new"])
        assert result.exit_code == 0
        assert "renamed" in result.output

    def test_convert_alias(self, runner, v2_project):
        result = runner.invoke(main, ["cv", "tree:month"])
        assert result.exit_code == 0
