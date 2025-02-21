import logging
import os
from datetime import datetime
import sqlite3
from scripts.database.db_statistics import DBStatistics 
from ..db_data_exchanger import DBDataExchanger
import math
from tqdm import tqdm
import time
from ..core.db_engine import ReaderWriterPair
from ..data_model.entities import EntityOccurrence, NamedEntity, EntityCooccurence
from ..data_model.docs import Docs
from ..data_model.sent import Sentence

class DBAnalysis:

    def __init__(
        self,
        conn: sqlite3.Connection,
        cursor: sqlite3.Cursor,
        logger: logging.Logger,
        data_exchanger: DBDataExchanger,
        log_query_plan,
        execute_with_log,
        conn_params_dict,


    ):
        self.conn = conn
        self.cursor = cursor
        self.logger = logger
        self.data_exchanger = data_exchanger
        self.statistics = DBStatistics(
            conn, cursor, logger, data_exchanger=data_exchanger
        )  # Initialize DBStatistics
        self.log_query_plan = log_query_plan
        self.execute_with_log = execute_with_log
        self.conn_params_dict = conn_params_dict
        self.named_entity = NamedEntity(self.conn, self.cursor, self.logger)
        self.entity_occurrence = EntityOccurrence(self.conn, self.cursor, self.logger, log_query_plan=self.log_query_plan, conn_params_dict=self.conn_params_dict)

        self.entity_cooccurence = EntityCooccurence(self.conn, self.cursor, self.logger)


    def aggregate_entity_occurrences(self) -> None:
        """
        Aggregate entity occurrences.
        """
        
    def suite_analysis(self) -> None:
        """
        Run a suite of analysis steps in sequence:
        - Entity co-occurrence summarization
        - Entity co-occurrence aggregation
        - PMI calculation
        """

        # Baseline analysis
        self.count_named_entity_fq()

        # Entity occurrence analysis 
        self.entity_occurrence.identify_overlap()
        self.aggregate_entity_occurrences()
        self.entity_occurrence.calc_intra_doc_fq()

        # Entity co-occurrence analysis
        self.count_entity_cooccurrences(level="document")
        self.co_aggregate_old()
        self.co_calc_pmi()


