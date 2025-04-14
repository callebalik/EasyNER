import pytest
import os
import tempfile
import json
from easyner.io import get_io_handler


class TestIOHandlerIntegration:
    """Integration tests for IO handlers using actual pipeline data structures."""

    @pytest.fixture
    def sample_pubmed_data(self):
        # Path to the sample data file from the splitter test directory
        sample_input_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "pipeline", "splitter", "pubmed_output.json"
        )

        with open(sample_input_path, "r") as f:
            return json.load(f)

    def test_json_handler_with_pubmed_data(self, sample_pubmed_data):
        """Test that JsonHandler can read and write actual pubmed data."""
        handler = get_io_handler("json")

        with tempfile.TemporaryDirectory() as tmp_dir:
            # Write the sample data to a temporary file
            test_file = os.path.join(tmp_dir, "pubmed-123.json")
            handler.write(sample_pubmed_data, test_file)

            # Read it back
            read_data = handler.read(test_file)

            # Verify data integrity
            assert read_data == sample_pubmed_data

            # Check some specific elements to ensure structure is maintained
            assert "30981" in read_data
            assert (
                read_data["30981"]["title"]
                == "Potentiation of apomorphine action in rats by l-prolyl-l-leucyl-glycine amide."
            )
            assert len(read_data["30981"]["sentences"]) == 4
            assert read_data["30981"]["sentences"][0]["text"].startswith(
                "Although the antiparkinsonian"
            )
