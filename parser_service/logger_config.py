import logging
from logging.config import dictConfig


DEFAULT_LEVEL = "DEBUG"

logging_schema = {
    "version": 1,
    "formatters": {
        "standard": {
            "class": "logging.Formatter",
            "format": '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            "datefmt": "%d %b %y %H:%M:%S"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "level": DEFAULT_LEVEL,
            "stream": "ext://sys.stdout"
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "standard",
            "level": DEFAULT_LEVEL,
            "filename": "parser_log.log",
            "mode": "a",
            "encoding": "utf-8",
        }
    },
    "loggers": {
        "__main__": {
            "handlers": ["console", "file"],
            "level": DEFAULT_LEVEL,
            "propagate": False
        }
    },
    "root": {
        "level": DEFAULT_LEVEL,
        "handlers": ["console", "file"]
    }
}

dictConfig(logging_schema)
parser_logger = logging.getLogger(__name__)
