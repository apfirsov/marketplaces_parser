import logging
from logging.config import dictConfig

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
            "level": "INFO",
            "stream": "ext://sys.stdout"
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "standard",
            "level": "INFO",
            "filename": "parser_log.log",
            "mode": "a",
            "encoding": "utf-8",
            "maxBytes": 500000,
            "backupCount": 4
        }
    },
    "loggers": {
        "__main__": {
            "handlers": ["console", "file"],
            "level": "INFO",
            "propagate": False
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["console", "file"]
    }
}

dictConfig(logging_schema)
parser_logger = logging.getLogger(__name__)
