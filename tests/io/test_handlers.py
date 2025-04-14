import pytest
import os
import json
import tempfile
from easyner.io.handlers import JsonHandler, ParquetHandler


class TestJsonHandler:

    def test_extension_constant(self):
        assert JsonHandler.EXTENSION == "json"

    def test_read_valid_json(self):
        handler = JsonHandler()

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a test JSON file
            test_data = {"key1": "value1", "key2": {"nested": "value2"}}
            test_file = os.path.join(tmp_dir, "test_file.json")

            with open(test_file, "w", encoding="utf-8") as f:
                json.dump(test_data, f)

                def test_read_empty_json_file(self):
                    handler = JsonHandler()
                    with tempfile.TemporaryDirectory() as tmp_dir:
                        empty_file = os.path.join(tmp_dir, "empty.json")
                        # Create an empty file
                        with open(empty_file, "w", encoding="utf-8"):
                            pass
                        with pytest.raises(ValueError, match="Empty file detected"):
                            handler.read(empty_file)

                def test_write_and_read_large_json(self):
                    handler = JsonHandler()
                    with tempfile.TemporaryDirectory() as tmp_dir:
                        large_data = {"numbers": list(range(10000)), "text": "a" * 10000}
                        test_file = os.path.join(tmp_dir, "large.json")
                        handler.write(large_data, test_file)
                        result = handler.read(test_file)
                        assert result == large_data

                def test_write_invalid_path(self):
                    handler = JsonHandler()
                    # Use an invalid directory path
                    invalid_path = "/invalid_dir/output.json"
                    with pytest.raises(Exception):
                        handler.write({"a": 1}, invalid_path)

                def test_read_with_timeout(self):
                    handler = JsonHandler()
                    with tempfile.TemporaryDirectory() as tmp_dir:
                        test_data = {"key": "value"}
                        test_file = os.path.join(tmp_dir, "test.json")
                        with open(test_file, "w", encoding="utf-8") as f:
                            json.dump(test_data, f)
                        # Should not timeout for small file
                        result = handler.read(test_file, timeout=1)
                        assert result == test_data

                def test_write_with_indent_and_read(self):
                    handler = JsonHandler()
                    with tempfile.TemporaryDirectory() as tmp_dir:
                        data = {"foo": [1, 2, 3], "bar": {"baz": True}}
                        test_file = os.path.join(tmp_dir, "pretty.json")
                        handler.write(data, test_file, indent=4)
                        with open(test_file, "r", encoding="utf-8") as f:
                            content = f.read()
                        assert content.count("\n") > 0
                        result = handler.read(test_file)
                        assert result == data

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create an invalid JSON file
            test_file = os.path.join(tmp_dir, "invalid.json")

            with open(test_file, "w", encoding="utf-8") as f:
                f.write('{"key": "value", invalid')

            # Should raise ValueError for invalid JSON
            with pytest.raises(ValueError, match="Error decoding JSON file"):
                handler.read(test_file)

    def test_read_nonexistent_file(self):
        handler = JsonHandler()

        with tempfile.TemporaryDirectory() as tmp_dir:
            non_existent_file = os.path.join(tmp_dir, "non_existent.json")

            # Should raise FileNotFoundError
            with pytest.raises(FileNotFoundError):
                handler.read(non_existent_file)

    def test_write_json_no_indent(self):
        handler = JsonHandler()

        with tempfile.TemporaryDirectory() as tmp_dir:
            test_data = {"key1": "value1", "key2": [1, 2, 3]}
            test_file = os.path.join(tmp_dir, "output.json")

            # Write data to file
            handler.write(test_data, test_file)

            # Read the file back and verify contents
            with open(test_file, "r", encoding="utf-8") as f:
                result = json.load(f)

            assert result == test_data

    def test_write_json_with_indent(self):
        handler = JsonHandler()

        with tempfile.TemporaryDirectory() as tmp_dir:
            test_data = {"key1": "value1", "key2": [1, 2, 3]}
            test_file = os.path.join(tmp_dir, "output_pretty.json")

            # Write data to file with indentation
            handler.write(test_data, test_file, indent=2)

            # Read the file back and verify contents
            with open(test_file, "r", encoding="utf-8") as f:
                result = json.load(f)

            assert result == test_data

            # Also check if it's pretty-printed (indented)
            with open(test_file, "r", encoding="utf-8") as f:
                content = f.read()

            # Pretty-printed JSON should span multiple lines
            assert content.count("\n") > 0


class TestParquetHandler:

    def test_extension_constant(self):
        assert ParquetHandler.EXTENSION == "parquet"

    @pytest.mark.xfail(reason="Parquet reading not implemented yet")
    def test_read_not_implemented(self):
        handler = ParquetHandler()

        with pytest.raises(NotImplementedError):
            handler.read("dummy.parquet")

    @pytest.mark.xfail(reason="Parquet writing not implemented yet")
    def test_write_not_implemented(self):
        handler = ParquetHandler()

        with pytest.raises(NotImplementedError):
            handler.write({"test": "data"}, "dummy.parquet")
