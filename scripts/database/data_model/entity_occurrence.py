# entity_cooccurrence_module.py
import sqlite3

from .schema import *
import logging
from ..db_data_exchanger import DBDataExchanger
class BaseComponent:
    """ Common base class for all components with shared logger and database connection. """
    def init_deps(self, db_system : EasyNerDBHandler,):
        self.logger = db_system.logger
        self.cursor = db_system.cursor
        self.conn = db_system.conn
        self.log_query_plan = db_system.log_query_plan
        self.conn_params_dict = db_system.conn_params_dict
        self.parent = db_system
        self.data : DBDataExchanger = db_system.data_exchanger  # Add this line
        return self

class SchemaManager(BaseComponent):
    def __init__(self, parent):
        super().init_deps(parent)


    def setup_tables(self):
        """Create tables for entity occurrences if they don't exist"""
        self.logger.info("Setting up tables for entity occurrence analysis...")

        self.cursor.execute(schema_create_table_docs)
        self.cursor.execute(schema_create_table_sentences)
        self.cursor.execute(schema_create_table_ne_lookup)
        self.cursor.execute(schema_create_table_ne_aggregated)
        self.cursor.execute(schema_create_table_ne_class)
        self.cursor.execute(schema_create_table_ne_error)
        self.cursor.execute(schema_create_table_ne)

        self.conn.commit()

        self.logger.info("Tables created successfully.")

                SELECT
                    COUNT(*) as total_pairs,
                    (SELECT COUNT(DISTINCT entity_id)
                        FROM entity_occurrences
                        WHERE id IN (SELECT e1_id FROM entity_cooccurrences
                                            UNION
                                            SELECT e2_id FROM entity_cooccurrences)) as total_entities,
                    COALESCE(AVG(sentence_distance), 0) as avg_distance
                FROM entity_cooccurrences
                """
            )
            total_stats = cursor.fetchone()
            logger.info(
                f"\nCo-occurrence identification complete:"
                f"\n- Total unique pairs: {total_stats[0]:,}"
                f"\n- Unique entities involved: {total_stats[1]:,}"
                + (
                    f"\n- Average sentence distance: {total_stats[2]:.2f}"
                    if level_local == "sentence"
                    else ""
                )
            )
            return None

        base_executor.execute_operation(lambda conn: operation(conn, level),)


    def count_entity_cooccurrences_multithreaded(self, level: str = "document", batch_size=5000, num_reader_threads=32) -> None:
        """
class Analysis(BaseComponent):
    def __init__(self, parent):
        super().init_deps(parent)
class Statistics(BaseComponent):
    def __init__(self, parent):
        super().init_deps(parent)
            return None

class Tests(BaseComponent):
    def __init__(self, parent, stats: Statistics):
        super().init_deps(parent)
        self.stats: Statistics = stats

class EntityOccurrence():
    """
    Main entrypoint class for Entity Occurrence functionality.
    Handles integration of schema management, analysis, statistics, and testing.
    """
    def __init__(self, db_system_instance):
        self.db_system = db_system_instance
        self.data_exchanger = db_system_instance.data_exchanger
        self.schema_manager = SchemaManager(self)
        self.preprocessor = Preprocessor(self)
        self.analysis = Analysis(self)
        self.statistics = Statistics(self)
        self.tests = Tests(self, self.statistics)

    @property
    def conn(self):
        return self.db_system.conn

    @property
    def logger(self):
        return self.db_system.logger

    @property
    def cursor(self):
        return self.db_system.cursor

    @property
    def conn_params_dict(self):
        return self.db_system.conn_params_dict

    def setup(self):
        """Initialize the entity occurrence system by setting up tables"""
        self.schema_manager.setup_tables()

    def record_occurrences(self, *args, **kwargs):
        """Delegate to analysis component"""
        if self.analysis.record_entity_occurrences(*args, **kwargs):
            self.tests.test_entity_occurrences()

