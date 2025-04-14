import pytest
import os
import tempfile
from pathlib import Path
from easyner.io.base import IOHandler


# Create a concrete implementation of IOHandler for testing
class TestHandler(IOHandler):
    def read(self, file_path, **kwargs):
        return {"test": "data"}

    def write(self, data, file_path, **kwargs):
        with open(file_path, "w", encoding=self.encoding) as f:
            f.write("test")


class TestIOHandlerBase:

    def test_init_default_encoding(self):
        handler = TestHandler()
        assert handler.encoding == IOHandler.DEFAULT_ENCODING

    def test_init_custom_encoding(self):
        custom_encoding = "latin-1"
        handler = TestHandler(encoding=custom_encoding)
        assert handler.encoding == custom_encoding

    def test_ensure_dir_exists(self):
        handler = TestHandler()

        with tempfile.TemporaryDirectory() as tmp_dir:
            test_dir = os.path.join(tmp_dir, "test_dir")
            test_file = os.path.join(test_dir, "test_file.json")

            # Directory should not exist initially
            assert not os.path.exists(test_dir)

            # Call ensure_dir_exists
            handler.ensure_dir_exists(test_file)

            # Directory should exist now
            assert os.path.exists(test_dir)

            # Calling again should not raise an error
            handler.ensure_dir_exists(test_file)

    def test_check_file_exists(self):
        handler = TestHandler()

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a test file
            test_file = os.path.join(tmp_dir, "test_file.json")
            with open(test_file, "w") as f:
                f.write("test")

            # Should not raise for existing file
            handler.check_file_exists(test_file)

            # Should raise for non-existing file
            non_existent_file = os.path.join(tmp_dir, "non_existent.json")
            with pytest.raises(FileNotFoundError):
                handler.check_file_exists(non_existent_file)
