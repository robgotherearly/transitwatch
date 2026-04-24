# ingestion/tests/conftest.py
# Shared pytest config — adds ingestion/ to sys.path so imports work.

import sys
import os

# Allow imports like `from gtfs_parser import ...` in tests
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
