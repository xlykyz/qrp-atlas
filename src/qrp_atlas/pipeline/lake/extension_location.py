"""Dagster extension code-location entry point (explicit deployment only)."""
from .orchestration import configured_definitions

defs = configured_definitions("extension")
