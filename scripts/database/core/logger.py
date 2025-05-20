import logging


class BaseLogger:
    """Base class for application logging with configurable file and console handlers."""

    def __init__(
        self,
        logger_name="BaseAppLogger",
        log_level=logging.DEBUG,
        log_dir="logs",
        enable_console_log=True,
    ):
        """
        Initialize the BaseLogger.

        :param logger_name: Name of the logger.
        :param log_level: Default logging level for file handler.
        :param log_dir: Directory to store log files.
        :param enable_console_log: Boolean to enable or disable console logging.
        """
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(
            logging.DEBUG
        )  # Logger level is set to DEBUG to capture all messages, handlers can filter

        # Ensure the logs directory exists
        os.makedirs(log_dir, exist_ok=True)
        self.log_dir = log_dir
        self.logger_name = logger_name

        self._setup_file_handler(log_level)
        if enable_console_log:
            self._setup_console_handler()
        self._setup_error_file_handler()

    def _get_log_formatter(self):
        """Returns a consistent log formatter."""
        return logging.Formatter(
            "%(asctime)s - %(name)s - [%(threadName)s] - %(levelname)s - %(message)s"
        )

    def _setup_file_handler(self, log_level):
        """Sets up file handler for logging all levels to a file."""
        log_file = os.path.join(self.log_dir, self.logger_name + ".log")
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)  # Set level from constructor argument
        file_handler.setFormatter(self._get_log_formatter())
        self.logger.addHandler(file_handler)

    def _setup_console_handler(self):
        """Sets up console handler for logging INFO level and above."""
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(self._get_log_formatter())
        self.logger.addHandler(console_handler)

    def _setup_error_file_handler(self):
        """Sets up a separate file handler for logging ERROR level and above to an error log file."""
        error_log_file = os.path.join(
            self.log_dir, "db_error.log"
        )  # Keeping "db_error.log" as in original
        error_file_handler = logging.FileHandler(error_log_file)
        error_file_handler.setLevel(logging.ERROR)
        error_file_handler.setFormatter(self._get_log_formatter())
        self.logger.addHandler(error_file_handler)

    def log_debug(self, message):
        """Log a debug message."""
        self.logger.debug(message)

    def log_info(self, message):
        """Log an info message."""
        self.logger.info(message)

    def log_warning(self, message):
        """Log a warning message."""
        self.logger.warning(message)

    def log_error(self, message):
        """Log an error message."""
        self.logger.error(message)

    def log_critical(self, message):
        """Log a critical message."""
        self.logger.critical(message)
