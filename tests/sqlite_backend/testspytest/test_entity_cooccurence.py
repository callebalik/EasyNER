from unittest.mock import MagicMock, patch

import pytest

from easyner.database.sqlite_backend.data_model.entity_cooccurrence import (
    Cooccurrence,
    NormallizedNamedEntity,
)
from easyner.database.sqlite_backend.data_model.schema import (
    AVG_SENT_DIST,
    CO_AGGR_ID,
    E1,
    E1_NORM_ID,
    E2,
    E2_NORM_ID,
    FQ,
    FQ_DOCUMENT_LEVEL,
    FQ_SENTENCE_LEVEL,
    MAX_SENT_DIST,
    MIN_SENT_DIST,
    NE_CLASS,
    NE_NORM_ID,
    NPMI,
    PMI,
    TXT,
    UNIQ_DOCS,
)


# Fixtures for mock objects
@pytest.fixture
def mock_entity1():
    """Create a sample normalized entity."""
    return NormallizedNamedEntity(
        txt="Diabetes",
        norm_id=1,
        fq=50,
        uniq_docs=20,
        ne_class="DISEASE",
    )


@pytest.fixture
def mock_entity2():
    """Create a sample normalized entity."""
    return NormallizedNamedEntity(
        txt="Fatigue",
        norm_id=2,
        fq=30,
        uniq_docs=15,
        ne_class="PHENOMENON",
    )


@pytest.fixture
def sample_cooccurrence(
    mock_entity1: NormallizedNamedEntity,
    mock_entity2: NormallizedNamedEntity,
):
    """Create a sample cooccurrence object."""
    return Cooccurrence(
        e1=mock_entity1,
        e2=mock_entity2,
        co_aggr_id=1,
        fq_doc_level=10,
        fq_sent_level=8,
        uniq_docs=5,
        pmi=0.75,
        npmi=0.25,
        avg_sent_dist=1.5,
        min_sent_dist=0,
        max_sent_dist=3,
    )


# Fixtures for mock objects
@pytest.fixture
def mock_entity1():
    """Create a sample normalized entity."""
    return NormallizedNamedEntity(
        txt="Diabetes",
        norm_id=1,
        fq=50,
        uniq_docs=20,
        ne_class="DISEASE",
    )


@pytest.fixture
def mock_entity2():
    """Create a sample normalized entity."""
    return NormallizedNamedEntity(
        txt="Fatigue",
        norm_id=2,
        fq=30,
        uniq_docs=15,
        ne_class="PHENOMENON",
    )


@pytest.fixture
def sample_cooccurrence(mock_entity1, mock_entity2):
    """Create a sample cooccurrence object."""
    return Cooccurrence(
        e1=mock_entity1,
        e2=mock_entity2,
        co_aggr_id=1,
        fq_doc_level=10,
        fq_sent_level=8,
        uniq_docs=5,
        pmi=0.75,
        npmi=0.25,
        avg_sent_dist=1.5,
        min_sent_dist=0,
        max_sent_dist=3,
    )


@pytest.fixture
def mock_cursor():
    """Create a mock cursor with sample description."""
    cursor = MagicMock()
    cursor.description = [
        (E1_NORM_ID, None, None, None, None, None, None),  # noqa: F821
        (E2_NORM_ID, None, None, None, None, None, None),
        (E1 + "_" + TXT, None, None, None, None, None, None),
        (E2 + "_" + TXT, None, None, None, None, None, None),
        (E1 + "_" + FQ, None, None, None, None, None, None),
        (E2 + "_" + FQ, None, None, None, None, None, None),
        (E1 + "_" + UNIQ_DOCS, None, None, None, None, None, None),
        (E2 + "_" + UNIQ_DOCS, None, None, None, None, None, None),
        (CO_AGGR_ID, None, None, None, None, None, None),
        (FQ_DOCUMENT_LEVEL, None, None, None, None, None, None),
        (FQ_SENTENCE_LEVEL, None, None, None, None, None, None),
        (UNIQ_DOCS, None, None, None, None, None, None),
        (PMI, None, None, None, None, None, None),
        (NPMI, None, None, None, None, None, None),
        (AVG_SENT_DIST, None, None, None, None, None, None),  # noqa: F821
        (MIN_SENT_DIST, None, None, None, None, None, None),
        (MAX_SENT_DIST, None, None, None, None, None, None),  # noqa: F821
    ]
    return cursor


@pytest.fixture
def mock_row():
    """Create a mock row with sample data matching the cursor description."""
    return [
        1,  # E1_NORM_ID
        2,  # E2_NORM_ID
        "Diabetes",  # E1_TXT
        "Fatigue",  # E2_TXT
        50,  # E1_FQ
        30,  # E2_FQ
        20,  # E1_UNIQ_DOCS
        15,  # E2_UNIQ_DOCS
        1,  # CO_AGGR_ID
        10,  # FQ_DOCUMENT_LEVEL
        8,  # FQ_SENTENCE_LEVEL
        5,  # UNIQ_DOCS
        0.75,  # PMI
        0.25,  # NPMI
        1.5,  # AVG_SENT_DIST
        0,  # MIN_SENT_DIST
        3,  # MAX_SENT_DIST
    ]


@pytest.fixture
def mock_row_missing_data():
    """Create a mock row with some missing data to test error handling."""
    return [
        1,  # E1_NORM_ID
        2,  # E2_NORM_ID
        "Diabetes",  # E1_TXT
        None,  # E2_TXT - Missing
        50,  # E1_FQ
        30,  # E2_FQ
        20,  # E1_UNIQ_DOCS
        15,  # E2_UNIQ_DOCS
        None,  # CO_AGGR_ID - Missing
        10,  # FQ_DOCUMENT_LEVEL
        8,  # FQ_SENTENCE_LEVEL
        5,  # UNIQ_DOCS
        None,  # PMI - Missing
        None,  # NPMI - Missing
        1.5,  # AVG_SENT_DIST
        0,  # MIN_SENT_DIST
        3,  # MAX_SENT_DIST
    ]


class TestNormallizedNamedEntity:
    def test_initialization(self, mock_entity1) -> None:
        """Test basic initialization of NormallizedNamedEntity."""
        assert mock_entity1.txt == "Diabetes"
        assert mock_entity1.norm_id == 1
        assert mock_entity1.fq == 50
        assert mock_entity1.uniq_docs == 20
        assert mock_entity1.ne_class == "DISEASE"

    def test_to_dict(self, mock_entity1) -> None:
        """Test to_dict method correctly serializes entity data."""
        result = mock_entity1.to_dict()

        assert result[TXT] == "Diabetes"
        assert result[NE_NORM_ID] == 1
        assert result[FQ] == 50
        assert result[UNIQ_DOCS] == 20
        assert result[NE_CLASS] == "DISEASE"


class TestCooccurrence:
    def test_initialization(self, sample_cooccurrence) -> None:
        """Test basic initialization of Cooccurrence."""
        assert sample_cooccurrence.e1.txt == "Diabetes"
        assert sample_cooccurrence.e2.txt == "Fatigue"
        assert sample_cooccurrence.co_aggr_id == 1
        assert sample_cooccurrence.fq_doc_level == 10
        assert sample_cooccurrence.fq_sent_level == 8
        assert sample_cooccurrence.uniq_docs == 5
        assert sample_cooccurrence.pmi == 0.75
        assert sample_cooccurrence.npmi == 0.25
        assert sample_cooccurrence.avg_sent_dist == 1.5
        assert sample_cooccurrence.min_sent_dist == 0
        assert sample_cooccurrence.max_sent_dist == 3

    def test_row_factory_success(self, mock_cursor, mock_row) -> None:
        """Test row_factory successfully creates Cooccurrence from valid data."""
        result = Cooccurrence.row_factory(mock_cursor, mock_row)

        assert isinstance(result, Cooccurrence)
        assert result.e1.txt == "Diabetes"
        assert result.e2.txt == "Fatigue"
        assert result.co_aggr_id == 1
        assert result.fq_doc_level == 10
        assert result.fq_sent_level == 8
        assert result.uniq_docs == 5
        assert result.pmi == 0.75
        assert result.npmi == 0.25
        assert result.avg_sent_dist == 1.5
        assert result.min_sent_dist == 0
        assert result.max_sent_dist == 3

    def test_row_factory_with_missing_data(
        self,
        mock_cursor,
        mock_row_missing_data,
    ) -> None:
        """Test row_factory handles missing data gracefully."""
        result = Cooccurrence.row_factory(mock_cursor, mock_row_missing_data)

        assert isinstance(result, Cooccurrence)
        assert result.e1.txt == "Diabetes"
        assert result.e2.txt is None  # This should be None
        assert result.co_aggr_id is None  # This should be None
        assert result.pmi is None  # This should be None
        assert result.npmi is None  # This should be None

    @patch(
        "easyner.database.sql_backend.data_model.entity_cooccurrence.NormallizedNamedEntity",
    )
    def test_row_factory_entity_error(
        self,
        mock_entity_class,
        mock_cursor,
        mock_row,
    ) -> None:
        """Test row_factory handles errors during entity creation."""
        # Make the NormallizedNamedEntity constructor raise an error
        mock_entity_class.side_effect = AttributeError("Test error")

        result = Cooccurrence.row_factory(mock_cursor, mock_row)
        assert result is None

    def test_to_dict_method(self, sample_cooccurrence) -> None:
        """Test to_dict method once implemented."""
        result = sample_cooccurrence.to_dict()

        assert result["entity1"] == sample_cooccurrence.e1.to_dict()
        assert result["entity2"] == sample_cooccurrence.e2.to_dict()
        assert result["fq_doc_level"] == 10
        assert result["fq_sent_level"] == 8
        assert result["uniq_docs"] == 5
        assert result["pmi"] == 0.75
        assert result["npmi"] == 0.25
        assert result["avg_sent_dist"] == 1.5
        assert result["min_sent_dist"] == 0
        assert result["max_sent_dist"] == 3
