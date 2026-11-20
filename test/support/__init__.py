"""Shared offline fixtures and repository paths; importing this starts no tests."""

from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
"""Path to the checkout containing utilities, examples, and the test tree."""
