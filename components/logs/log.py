import logging
import json
import traceback
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta

SUCCESS_LEVEL = 25
CRITICAL_LEVEL = 50
LOG_COLORS = {
    "DEBUG": "\033[94m",
    "INFO": "\033[96m",
    "SUCCESS": "\033[92m",
    "WARNING": "\033[93m",
    "ERROR": "\033[91m",
    "CRITICAL": "\033[95m",
    "RESET": "\033[0m",
    "BOLD": "\033[1m",
}

logging.addLevelName(SUCCESS_LEVEL, "SUCCESS")
logging.addLevelName(CRITICAL_LEVEL, "CRITICAL")


class JSONFormatter(logging.Formatter):
    def __init__(self, text):
        super().__init__()
        self.text = text

    def format(self, record):
        exc_text = traceback.format_exc() if record.exc_info else None
        log_entry = {
            "text": self.text,
            "record": {
                "elapsed": {
                    "repr": str(timedelta(seconds=record.relativeCreated / 1000)),
                    "seconds": record.relativeCreated / 1000,
                },
                "exception": exc_text,
                "extra": record.__dict__.get("extra", {}),
                "file": {"name": record.filename, "path": record.pathname},
                "function": record.funcName,
                "level": {
                    "icon": "✅"
                    if record.levelno == SUCCESS_LEVEL
                    else "ℹ️"
                    if record.levelno == logging.INFO
                    else "⚠️",
                    "name": record.levelname,
                    "no": record.levelno,
                },
                "line": record.lineno,
                "message": record.getMessage(),
                "module": record.module,
                "name": record.name,
                "process": {"id": record.process, "name": record.processName},
                "thread": {"id": record.thread, "name": record.threadName},
                "time": {
                    "repr": datetime.utcfromtimestamp(record.created).isoformat()
                    + "+00:00",
                    "timestamp": record.created,
                },
            },
        }
        return json.dumps(log_entry)


class PlainTextFormatter(logging.Formatter):
    def format(self, record):
        exc_text = traceback.format_exc() if record.exc_info else None
        log_time = datetime.utcfromtimestamp(record.created).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3]
        level_color = LOG_COLORS.get(record.levelname, LOG_COLORS["RESET"])
        message_bold = f"{LOG_COLORS['BOLD']}{exc_text or record.getMessage()}{LOG_COLORS['RESET']}"
        return f"{log_time} | {LOG_COLORS['BOLD']}{level_color}{record.levelname:<8}{LOG_COLORS['RESET']} | {record.funcName}:{record.lineno} - {message_bold}"


class Logger:
    def __init__(self):
        self.logger = logging.getLogger("custom_logger")
        self.logger.setLevel(logging.DEBUG)
        stdout_handler = logging.StreamHandler()
        stdout_handler.setLevel(logging.DEBUG)
        stdout_handler.setFormatter(PlainTextFormatter())
        self.logger.addHandler(stdout_handler)

    def add(self, filepath, level, colorize, max_size_mb, retention, text, serialize):
        for handler in self.logger.handlers[:]:
            if isinstance(handler, RotatingFileHandler):
                self.logger.removeHandler(handler)
                handler.close()
        handler = RotatingFileHandler(
            filepath,
            maxBytes=max_size_mb * 1024 * 1024,
            backupCount=retention,
            encoding="utf-8",
        )
        handler.setLevel(level)
        handler.setFormatter(JSONFormatter(text(None)))
        self.logger.addHandler(handler)

    def log(self, level, message, exc_info=None):
        self.logger.log(level, message, exc_info=exc_info, stacklevel=2)

    def info(self, message, exc_info=None):
        self.logger.info(message, exc_info=exc_info, stacklevel=2)

    def warning(self, message, exc_info=None):
        self.logger.warning(message, exc_info=exc_info, stacklevel=2)

    def error(self, message, exc_info=None):
        self.logger.error(message, exc_info=exc_info, stacklevel=2)

    def debug(self, message, exc_info=None):
        self.logger.debug(message, exc_info=exc_info, stacklevel=2)

    def success(self, message, exc_info=None):
        self.logger.log(SUCCESS_LEVEL, message, exc_info=exc_info, stacklevel=2)

    def critical(self, message, exc_info=None):
        self.logger.log(CRITICAL_LEVEL, message, exc_info=exc_info, stacklevel=2)
