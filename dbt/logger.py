import logging
import logging.config
import os

def make_log_dir_if_missing(log_dir):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

def getLogger(log_dir, name):
    make_log_dir_if_missing(log_dir)
    filename = "dbt.log"
    base_log_path = os.path.join(log_dir, filename)

    dictLogConfig = {
        "version":1,
        "handlers": {
            "fileHandler":{
                "class":"logging.handlers.TimedRotatingFileHandler",
                "formatter":"fileFormatter",
                "when": "d",  # rotate daily
                "interval": 1,
                "backupCount": 7,
                "filename": base_log_path
            },
        },
        "loggers":{
            "dbt":{
                "handlers":["fileHandler"],
                "level":"DEBUG",
                "propagate": False
            }
        },

        "formatters":{
            "fileFormatter":{
                "format":"%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s"
            }
        }
    }
    logging.config.dictConfig(dictLogConfig)
    logger = logging.getLogger(name)
    return logger

