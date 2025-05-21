from unittest.mock import MagicMock, patch

import pytest

from easyner.io.database.services.data_exchanger import DataExchanger


@pytest.fixture
def mock_db_handler():
    db_handler = MagicMock()
    db_handler.cursor = MagicMock()
    db_handler.logger = MagicMock()
    return db_handler


@pytest.fixture
def data_exchanger(mock_db_handler):
    with patch(
        "easyner.database.sql_backend.data_model.entity_cooccurrence.VIEW_DIS_PNM_CO_AGGR_ROW_FACTORY",
    ) as mock_view:
        mock_view.refresh = MagicMock()
        exchanger = DataExchanger(mock_db_handler)
        return exchanger


class TestDataExchangerFilters:
    def test_empty_filters(self, data_exchanger) -> None:
        """Test get_cooccurrences with empty filters."""
        data_exchanger.get_cooccurrences({})
        # Should have called execute with just the base query, limit and offset
        data_exchanger.cursor.execute.assert_called_once()
        args, _ = data_exchanger.cursor.execute.call_args
        query = args[0]
        params = args[1] if len(args) > 1 else []

        assert "WHERE" not in query
        assert "LIMIT 100" in query
        assert "OFFSET 0" in query
        assert len(params) == 0

    def test_equality_filter(self, data_exchanger) -> None:
        """Test get_cooccurrences with simple equality filter."""
        data_exchanger.get_cooccurrences({"e1_norm_id": 5})

        # Should have called execute with WHERE clause
        data_exchanger.cursor.execute.assert_called_once()
        args, _ = data_exchanger.cursor.execute.call_args
        query = args[0]
        params = args[1] if len(args) > 1 else []

        assert "WHERE e1_norm_id = ?" in query
        assert params[0] == 5

    def test_like_filter(self, data_exchanger) -> None:
        """Test get_cooccurrences with LIKE filter."""
        data_exchanger.get_cooccurrences({"e1_txt_like": "diabetes%"})

        # Should have called execute with LIKE clause
        data_exchanger.cursor.execute.assert_called_once()
        args, _ = data_exchanger.cursor.execute.call_args
        query = args[0]
        params = args[1] if len(args) > 1 else []

        assert "WHERE e1_txt LIKE ?" in query
        assert params[0] == "diabetes%"

    def test_min_max_filters(self, data_exchanger) -> None:
        """Test get_cooccurrences with min and max filters resulting in BETWEEN."""
        data_exchanger.get_cooccurrences({"min_npmi": 0.3, "max_npmi": 0.8})

        # Should have called execute with BETWEEN clause
        data_exchanger.cursor.execute.assert_called_once()
        args, _ = data_exchanger.cursor.execute.call_args
        query = args[0]
        params = args[1] if len(args) > 1 else []

        assert "WHERE npmi BETWEEN ? AND ?" in query
        assert params[0] == 0.3
        assert params[1] == 0.8

    def test_combined_filters(self, data_exchanger) -> None:
        """Test get_cooccurrences with combination of different filter types."""
        filters = {
            "e1_txt_like": "covid%",
            "min_npmi": 0.3,
            "max_npmi": 0.8,
            "min_fq_doc_level": 5,
            "e2_norm_id": 42,
        }
        data_exchanger.get_cooccurrences(filters)

        # Should have called execute with multiple clauses
        data_exchanger.cursor.execute.assert_called_once()
        args, _ = data_exchanger.cursor.execute.call_args
        query = args[0]
        params = args[1] if len(args) > 1 else []

        assert "WHERE" in query
        assert "e1_txt LIKE ?" in query
        assert "npmi BETWEEN ? AND ?" in query
        assert "fq_doc_level >= ?" in query
        assert "e2_norm_id = ?" in query
        assert "AND" in query
        assert len(params) == 5

    def test_handle_empty_filter_values(self, data_exchanger) -> None:
        """Test that empty filter values are properly handled."""
        filters = {"e1_txt_like": "", "min_npmi": None, "e2_norm_id": 42}
        data_exchanger.get_cooccurrences(filters)

        # Should only include non-empty filters
        data_exchanger.cursor.execute.assert_called_once()
        args, _ = data_exchanger.cursor.execute.call_args
        query = args[0]
        params = args[1] if len(args) > 1 else []

        assert "WHERE e2_norm_id = ?" in query
        assert len(params) == 1
        assert params[0] == 42
