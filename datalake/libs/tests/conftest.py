"""Make `datalake/libs` importable for the test suite.

Adds the parent of this file's parent (i.e. `datalake/libs/`) to sys.path so
tests can `import fabric_libs` and `import deltagen` without installing them.
"""
import os
import sys

_LIBS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _LIBS_DIR not in sys.path:
    sys.path.insert(0, _LIBS_DIR)
