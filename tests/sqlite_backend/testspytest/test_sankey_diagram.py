import os
import tempfile
from unittest.mock import MagicMock, patch

import plotly.graph_objects as go
import pytest

from easyner.database.sqlite_backend.data_model.entity_cooccurrence import (
    Cooccurrence,
    NormallizedNamedEntity,
)
from easyner.database.sqlite_backend.data_model.schema import (
    FQ_DOCUMENT_LEVEL,
    UNIQ_DOCS,
)
from easyner.database.sqlite_backend.statistics.sankey_diagram import CooccurenceSankey


@pytest.fixture
def mock_entities():
    """Create mock NormallizedNamedEntity objects for testing."""
    return {
        "covid": NormallizedNamedEntity(
            norm_id=1,
            txt="COVID-19",
            fq=100,
            uniq_docs=50,
            ne_class="DISEASE",
        ),
        "influenza": NormallizedNamedEntity(
            norm_id=2,
            txt="Influenza",
            fq=80,
            uniq_docs=40,
            ne_class="DISEASE",
        ),
        "fever": NormallizedNamedEntity(
            norm_id=101,
            txt="Fever",
            fq=90,
            uniq_docs=45,
            ne_class="PHENOMENON",
        ),
        "cough": NormallizedNamedEntity(
            norm_id=102,
            txt="Cough",
            fq=85,
            uniq_docs=42,
            ne_class="PHENOMENON",
        ),
    }


@pytest.fixture
def mock_cooccurrences(mock_entities):
    """Create mock Cooccurrence objects with valid data."""
    return [
        Cooccurrence(
            e1=mock_entities["covid"],
            e2=mock_entities["fever"],
            fq_doc_level=30,
            fq_sent_level=20,
            uniq_docs=25,
            npmi=0.6,
            pmi=2.5,
        ),
        Cooccurrence(
            e1=mock_entities["covid"],
            e2=mock_entities["cough"],
            fq_doc_level=25,
            fq_sent_level=18,
            uniq_docs=20,
            npmi=0.55,
            pmi=2.2,
        ),
        Cooccurrence(
            e1=mock_entities["influenza"],
            e2=mock_entities["fever"],
            fq_doc_level=22,
            fq_sent_level=15,
            uniq_docs=18,
            npmi=0.5,
            pmi=2.0,
        ),
    ]


@pytest.fixture
def mock_cooccurrences_with_none(mock_entities):
    """Create mock Cooccurrence objects with some None values to test edge cases."""
    return [
        Cooccurrence(
            e1=mock_entities["covid"],
            e2=mock_entities["fever"],
            fq_doc_level=None,  # None value to test handling
            fq_sent_level=20,
            uniq_docs=None,  # None value to test handling
            npmi=None,  # None value to test handling
            pmi=None,  # None value to test handling
        ),
    ]


@pytest.fixture
def sankey_diagram():
    """Create a sankey diagram instance for testing."""
    return CooccurenceSankey(logger=MagicMock())


class TestCooccurenceSankey:
    def test_process_data(self, sankey_diagram, mock_cooccurrences) -> None:
        """Test that _process_data correctly processes entity data."""
        result = sankey_diagram._process_data(mock_cooccurrences)

        # Verify data structure
        assert "diseases" in result
        assert "phenomena" in result
        assert "labels" in result
        assert len(result["diseases"]) == 2
        assert len(result["phenomena"]) == 2

        # Verify entity data was extracted correctly
        assert "COVID-19" in result["diseases"]
        assert "Fever" in result["phenomena"]

        # Verify frequency data was processed correctly
        assert result["disease_data"]["COVID-19"][FQ_DOCUMENT_LEVEL] == 100
        assert result["phenomenon_data"]["Fever"][UNIQ_DOCS] == 45

    def test_create_link_data(self, sankey_diagram, mock_cooccurrences) -> None:
        """Test that _create_link_data correctly generates link data."""
        processed_data = sankey_diagram._process_data(mock_cooccurrences)
        result = sankey_diagram._create_link_data(processed_data)

        # Verify all required data is present
        assert "source" in result
        assert "target" in result
        assert "value" in result
        assert "link_colors" in result
        assert len(result["source"]) == 3
        assert len(result["target"]) == 3

        # Verify link values use NPMI
        assert abs(result["value"][0] - 0.6) < 0.01

    def test_handle_none_values(
        self,
        sankey_diagram,
        mock_cooccurrences_with_none,
    ) -> None:
        """Test that the class handles None values gracefully."""
        processed_data = sankey_diagram._process_data(mock_cooccurrences_with_none)
        link_data = sankey_diagram._create_link_data(processed_data)
        node_colors = sankey_diagram._create_node_colors(processed_data)

        # Check that default values were used appropriately
        assert len(link_data["source"]) == 1
        assert len(link_data["target"]) == 1
        assert len(link_data["value"]) == 1
        assert len(node_colors["node_colors"]) == 2  # 1 diseases + 1 phenomena

    def test_create_sankey_diagram(self, sankey_diagram, mock_cooccurrences) -> None:
        """Test that create_sankey_diagram returns a valid Figure object."""
        # Test the core diagram creation
        fig = sankey_diagram.create_sankey_diagram(mock_cooccurrences)

        # Verify it returns a Plotly figure object
        assert isinstance(fig, go.Figure)

        # Check basic figure properties
        assert hasattr(fig, "data")
        assert len(fig.data) > 0
        assert isinstance(fig.data[0], go.Sankey)

        # Verify sankey data
        sankey_data = fig.data[0]
        assert len(sankey_data.node.label) == 4  # 2 diseases + 2 phenomena
        assert len(sankey_data.link.source) == 3  # 3 cooccurrences

    def test_empty_input(self, sankey_diagram) -> None:
        """Test behavior with empty input."""
        result = sankey_diagram.create_sankey_diagram([])
        # When cooccurrences list is empty, the method should return an HTML message
        assert isinstance(result, str)
        assert "No data available" in result

    def test_export_as_html_mock(self, sankey_diagram, mock_cooccurrences) -> None:
        """Test HTML export using mocks."""
        # Setup mock for write_html
        with patch.object(go.Figure, "write_html") as mock_write_html:
            # Call export method
            sankey_diagram.export_as_html(mock_cooccurrences, "test_output.html")

            # Verify write_html was called with correct filename
            mock_write_html.assert_called_once_with("test_output.html")

    def test_export_as_image_mock(self, sankey_diagram, mock_cooccurrences) -> None:
        """Test image export using mocks."""
        # Setup mock for write_image
        with patch.object(go.Figure, "write_image") as mock_write_image:
            # Call export method
            sankey_diagram.export_as_image(mock_cooccurrences, "test_output.png")

            # Verify write_image was called with correct filename
            mock_write_image.assert_called_once_with("test_output.png")

    def test_export_as_html_file(self, sankey_diagram, mock_cooccurrences) -> None:
        """Test actual HTML file generation."""
        # Create a temporary file for the test
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as tmp_file:
            filename = tmp_file.name

        try:
            # Export to the temporary file
            sankey_diagram.export_as_html(mock_cooccurrences, filename)

            # Verify the file exists and has content
            assert os.path.exists(filename)
            file_size = os.path.getsize(filename)
            assert file_size > 0

            # Verify it contains expected HTML content
            with open(filename) as f:
                content = f.read()
                assert "Plotly" in content
                assert "Sankey" in content

        finally:
            # Clean up - remove temporary file
            if os.path.exists(filename):
                os.unlink(filename)

    def test_export_as_image_file(self, sankey_diagram, mock_cooccurrences) -> None:
        """Test actual image file generation.

        Note: This test requires Plotly's kaleido package to be installed.
        """
        import importlib.util

        try:
            kaleido_spec = importlib.util.find_spec("kaleido")
            if kaleido_spec is None:
                kaleido_available = False
                pytest.skip(
                    "Kaleido not available - skipping actual image generation test",
                )
            else:
                kaleido_available = True
        except ImportError:
            kaleido_available = False
            pytest.skip("Kaleido not available - skipping actual image generation test")

        if kaleido_available:
            # Create a temporary file for the test
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp_file:
                filename = tmp_file.name

            try:
                # Export to the temporary file
                sankey_diagram.export_as_image(mock_cooccurrences, filename)

                # Verify the file exists and has content
                assert os.path.exists(filename)
                file_size = os.path.getsize(filename)
                assert file_size > 0

                # Additional verification could include checking PNG header bytes

            finally:
                # Clean up - remove temporary file
                if os.path.exists(filename):
                    os.unlink(filename)
