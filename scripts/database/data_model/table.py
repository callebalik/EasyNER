

from scripts.database.db_main import EasyNerDBHandler
from dataclasses import dataclass

@dataclass
class TableName:
    stem: str
    suffix: str
    db: EasyNerDBHandler

    @property
    def exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        """
        self.db.execute(
            f"""--sql
            SELECT COUNT(*)
            FROM sqlite_master
            WHERE type='table' AND name='{table_name}'
            """
        )
        return self.db.cursor.fetchone()[0] == 1

    @property
    def columns(self, table_name: str) -> list:
        """
        Get a list of columns in a table.
        """
        self.db.execute(
            f"""--sql
            PRAGMA table_info({table_name})
            """
        )
        return [row[1] for row in self.db.cursor.fetchall()]

