# analysis/analyzer.py
from ..core.core_classes import BaseExecutor, BaseLogger
from ..core.db_manager import DatabaseManager
from ..core.threading_mp.parallel_executor import ParallelExecutor

class DataAnalyzer(BaseExecutor):
    """Performs data analysis tasks."""

    def __init__(self, db_manager: DatabaseManager, logger: BaseLogger):
        super().__init__(db_manager, logger) # Initialize BaseExecutor
        self.parallel_executor = ParallelExecutor(db_manager, logger) # For parallel ops

    def analyze_user_activity(self, user_ids):
        
        """Analyzes activity for a list of user IDs in parallel."""
        self.logger.info(f"Starting user activity analysis for {len(user_ids)} users in parallel...")

        def _analyze_single_user(db_manager, logger, user_id):
            """Analyzes activity for a single user (task for parallel execution)."""
            logger.debug(f"Analyzing user activity for user ID: {user_id}")
            try:
                # Example database operations - replace with actual analysis queries
                with db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM user_actions WHERE user_id = ?", (user_id,))
                    action_count = cursor.fetchone()[0]
                    cursor.execute("SELECT AVG(duration) FROM user_sessions WHERE user_id = ?", (user_id,))
                    avg_session_duration = cursor.fetchone()[0] or 0 # Handle None

                analysis_result = {
                    "user_id": user_id,
                    "action_count": action_count,
                    "avg_session_duration": avg_session_duration
                }
                logger.debug(f"Analysis for user {user_id} completed: {analysis_result}")
                return analysis_result

            except Exception as e:
                logger.error(f"Error analyzing user {user_id}: {e}")
                return {"user_id": user_id, "error": str(e)}

        tasks = [lambda dbm, log, uid=uid: _analyze_single_user(dbm, log, uid) for uid in user_ids] # Create task lambdas
        results = self.parallel_executor.run_in_threads(tasks, max_workers=10) # Run in threads

        processed_results = [res for res in results if res] # Filter out None results (from errors)
        self.logger.info(f"User activity analysis completed. Analyzed users: {len(processed_results)} out of {len(user_ids)} requested.")
        return processed_results

    def get_average_data_value(self, table_name, column_name):
        """Example analysis function using BaseExecutor features."""
        def _fetch_average(conn, table, col): # Operation function
            cursor = conn.cursor()
            cursor.execute(f"SELECT AVG({col}) FROM {table}")
            return cursor.fetchone()[0]

        average_value = self.execute_operation(_fetch_average, table_name, column_name)
        self.logger.info(f"Average value of '{column_name}' in '{table_name}': {average_value}")
        return average_value

