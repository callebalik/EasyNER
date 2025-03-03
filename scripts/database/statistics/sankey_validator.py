import logging
import os
import sqlite3
from pathlib import Path

class SankeyValidator:
    """Validator for disease-phenomenon data relationships used in Sankey diagrams."""

    def __init__(self, db_path=None):
        """Initialize the validator with a database path."""
        self.logger = logging.getLogger('sankey_validator')
        self.db_path = db_path or os.environ.get('EASYNER_DB_PATH')

        if not self.db_path:
            self.logger.error("No database path provided or found in environment")
            raise ValueError("Database path not specified")

        if not Path(self.db_path).exists():
            self.logger.error(f"Database file not found: {self.db_path}")
            raise FileNotFoundError(f"Database file not found: {self.db_path}")

    def get_entity_classes(self):
        """Get all entity classes from the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT NE_CLASS_ID, NE_CLASS FROM NE_CLASS ORDER BY NE_CLASS")
                return cursor.fetchall()
        except Exception as e:
            self.logger.error(f"Error fetching entity classes: {e}")
            return []

    def find_matching_entity_classes(self, search_terms):
        """Find entity classes that match the given search terms (case insensitive)."""
        entity_classes = self.get_entity_classes()
        results = {}

        for class_id, class_name in entity_classes:
            for term in search_terms:
                if term.lower() in class_name.lower():
                    results[term.lower()] = {"id": class_id, "name": class_name}

        return results

    def test_sankey_query(self, class_filter1=None, class_filter2=None, min_npmi=0.0):
        """Test the Sankey query with optional filters and return sample results."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                query = """--sql
                SELECT
                    nea1.TXT_NORM AS entity1, nec1.NE_CLASS AS class1,
                    nea2.TXT_NORM AS entity2, nec2.NE_CLASS AS class2,
                    dpma.FQ_DOCUMENT_LEVEL AS frequency,
                    dpma.NPMI AS npmi
                FROM DIS_PNM_AGGR dpma
                JOIN NE_AGGR nea1 ON dpma.E1_NORM_ID = nea1.NE_NORM_ID
                JOIN NE_AGGR nea2 ON dpma.E2_NORM_ID = nea2.NE_NORM_ID
                JOIN NE_CLASS nec1 ON nea1.NE_CLASS_ID = nec1.NE_CLASS_ID
                JOIN NE_CLASS nec2 ON nea2.NE_CLASS_ID = nec2.NE_CLASS_ID
                WHERE dpma.FQ_DOCUMENT_LEVEL >= 1
                """

                params = []

                # Add class filters
                if class_filter1:
                    query += " AND nec1.NE_CLASS_ID = ?"
                    params.append(class_filter1)

                if class_filter2:
                    query += " AND nec2.NE_CLASS_ID = ?"
                    params.append(class_filter2)

                # Add NPMI filter
                if min_npmi > -1.0:
                    query += " AND dpma.NPMI >= ?"
                    params.append(min_npmi)

                query += " LIMIT 10"

                cursor.execute(query, params)
                return cursor.fetchall()

        except Exception as e:
            self.logger.error(f"Error testing Sankey query: {e}")
            return []

    def get_working_class_combinations(self):
        """Find entity class combinations that produce results."""
        entity_classes = self.get_entity_classes()
        working_combinations = []

        for i, (class1_id, class1_name) in enumerate(entity_classes):
            for class2_id, class2_name in entity_classes[i:]:
                results = self.test_sankey_query(class1_id, class2_id, -1.0)
                if results:
                    working_combinations.append({
                        "class1": {"id": class1_id, "name": class1_name},
                        "class2": {"id": class2_id, "name": class2_name},
                        "count": len(results),
                        "sample": results[0] if results else None
                    })

        return working_combinations