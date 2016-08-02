import logging
import logging.config

def getLogger(name):
    dictLogConfig = {
        "version":1,
        "handlers": {
            "fileHandler":{
                "class":"logging.FileHandler",
                "formatter":"fileFormatter",
                "filename":"dbt_output.log"
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

