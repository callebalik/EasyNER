import logging
from pathlib import Path
from textwrap import shorten
from typing import Any, Dict

from pydantic import Field, FilePath, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Set up logger for config operations
logger = logging.getLogger("EasyNerConfig")


class SQLiteConfig(BaseSettings):
    """Pydantic model for database configuration. Loads automatically from sources.

    Field value priority¶ from pydantic docs:

    In the case where a value is specified for the same Settings field in multiple ways, the selected value is determined as follows (in descending order of priority):

    1. Arguments passed to the Settings class initialiser.
    2. Environment variables, e.g. my_prefix_special_function as described above.
    3. Variables loaded from a dotenv (.env) file.
    4. Variables loaded from the secrets directory.
    5. The default field values for the Settings model.

    """

    model_config = SettingsConfigDict(
        env_prefix="SQLITE__",
        case_sensitive=False,
        extra="ignore",
    )

    db_path: FilePath = Field(description="SQLite database path")
    dev_mode: bool = Field(False, description="Development mode flag")
    schema_path: FilePath = Field(
        Path(__file__).parent / "schema.sql",
        description="database schema",
    )

    # Database PRAGMAS settings
    foreign_keys: bool = Field(True, description="Foreign keys constraint")
    journal_mode: str = Field("WAL", description="")
    synchronous: str = Field(
        "NORMAL",
        description="NORMAL for balanced safety/performance",
    )
    journal_size_limit: int = Field(6144000, description="In bytes")
    temp_store: str = Field(
        "MEMORY",
        description="Temp storage location (MEMORY for speed)",
    )
    busy_timeout: int = Field(10000, description="Busy timeout in ms")
    cache_size: int = Field(-2000, description="Cache size (<0 -> KiB, >0 -> pages)")
    mmap_size: int | None = Field(1073741824, description="Memory map (bytes)")

    log_level: str = Field("INFO", description="Logging level")

    @field_validator("db_path")
    @classmethod
    def validate_path(cls, value: str | None) -> str | None:
        """Validate that paths are absolute if provided."""
        if value and not Path(value).is_absolute():
            msg = f"Path must be absolute: {value}"
            raise ValueError(msg)

        return value

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        """Validate that the log level is one of the allowed values."""
        allowed_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if value not in allowed_levels:
            msg = f"Invalid log level: {value}. Allowed values are: {allowed_levels}"
            raise ValueError(msg)
        return value

    def apply_pragma_settings(self, connection) -> Dict[str, Any]:
        """Apply all pragma settings to the database connection.

        Args:
            connection: SQLite connection object

        Returns:
            Dict[str, Any]: Dictionary of applied settings for logging

        """
        settings = {}

        # Apply foreign keys setting
        foreign_keys_value = "ON" if self.foreign_keys else "OFF"
        connection.execute(f"PRAGMA foreign_keys = {foreign_keys_value};")
        settings["foreign_keys"] = foreign_keys_value

        # Apply journal mode
        connection.execute(f"PRAGMA journal_mode = {self.journal_mode};")
        settings["journal_mode"] = self.journal_mode

        # Apply synchronous mode
        connection.execute(f"PRAGMA synchronous = {self.synchronous};")
        settings["synchronous"] = self.synchronous

        # Apply journal size limit
        connection.execute(f"PRAGMA journal_size_limit = {self.journal_size_limit};")
        settings["journal_size_limit"] = self.journal_size_limit

        # Apply temp store location
        connection.execute(f"PRAGMA temp_store = {self.temp_store};")
        settings["temp_store"] = self.temp_store

        # Apply busy timeout
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout};")
        settings["busy_timeout"] = self.busy_timeout

        # Apply cache size
        connection.execute(f"PRAGMA cache_size = {self.cache_size};")
        settings["cache_size"] = self.cache_size

        # Apply memory mapping if enabled
        if self.mmap_size is not None:
            try:
                connection.execute(f"PRAGMA mmap_size = {self.mmap_size};")
                settings["mmap_size"] = self.mmap_size
            except Exception:
                # Memory mapping not supported on all systems
                settings["mmap_size"] = "not supported"

        return settings

    def __str__(self) -> str:
        """Return a formatted table of the configuration for print()."""
        try:
            config_items = self.model_dump()
            max_key_len = max(len(str(k)) for k in config_items)
            max_value_width = 50  # Maximum width for value column

            lines = []
            lines.append(
                "┌" + "─" * (max_key_len + 2) + "┬" + "─" * max_value_width + "┐",
            )
            lines.append(
                f"│ {'Setting'.ljust(max_key_len)} │ {'Value'.ljust(max_value_width - 2)} │",
            )
            lines.append(
                "├" + "─" * (max_key_len + 2) + "┼" + "─" * max_value_width + "┤",
            )

            for k, v in config_items.items():
                # Special handling for path values
                if k.endswith("_path") and v is not None:
                    # For path values, show the filename and truncate the directory if needed
                    try:
                        path_obj = Path(v)
                        filename = path_obj.name
                        directory = str(path_obj.parent)

                        # If directory is too long, truncate it
                        if len(directory) + len(filename) + 3 > max_value_width - 2:
                            directory_width = max_value_width - 2 - len(filename) - 3
                            if directory_width > 3:
                                directory = "..." + directory[-directory_width + 3 :]
                            else:
                                directory = "..."

                        v_str = f"{directory}/{filename}"

                        # Final truncation check
                        if len(v_str) > max_value_width - 2:
                            v_str = shorten(
                                v_str,
                                width=max_value_width - 2,
                                placeholder="…",
                            )
                    except:
                        # Fallback if path parsing fails
                        v_str = shorten(
                            str(v),
                            width=max_value_width - 2,
                            placeholder="…",
                        )
                else:
                    # Standard handling for non-path values
                    v_str = shorten(str(v), width=max_value_width - 2, placeholder="…")

                lines.append(
                    f"│ {str(k).ljust(max_key_len)} │ {v_str.ljust(max_value_width - 2)} │",
                )

            lines.append(
                "└" + "─" * (max_key_len + 2) + "┴" + "─" * max_value_width + "┘",
            )
            return "\n".join(lines)
        except Exception as e:
            # Fallback to basic representation if formatting fails
            logger.warning(f"Error formatting config table: {str(e)}")
            return f"SQLiteConfig({', '.join(f'{k}={v}' for k, v in self.model_dump().items() if not k.startswith('_'))})"


if __name__ == "__main__":
    settings = SQLiteConfig()
    print(settings)
    print(settings.schema_path)
