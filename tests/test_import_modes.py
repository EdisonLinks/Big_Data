import importlib
import sys
import unittest
from pathlib import Path


class ImportModeTest(unittest.TestCase):
    def test_repository_imports_when_webapp_directory_is_on_python_path(self):
        webapp_dir = Path(__file__).resolve().parents[1] / "webapp"
        sys.path.insert(0, str(webapp_dir))
        try:
            sys.modules.pop("repository", None)
            module = importlib.import_module("repository")
        finally:
            sys.path.remove(str(webapp_dir))

        self.assertTrue(hasattr(module, "MySQLRepository"))


if __name__ == "__main__":
    unittest.main()
