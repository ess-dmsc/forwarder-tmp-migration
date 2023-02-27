import logging
from typing import Optional

import graypy

logger_name = "forwarder"


def setup_logger(
    level: int = logging.DEBUG,
    log_file_name: Optional[str] = None,
    graylog_logger_address: Optional[str] = None,
    team: Optional[str] = "",
) -> None:
    global logger_name
    if team:
        logger_name = f"{team}-{logger_name}"
    if log_file_name is not None:
        logging.basicConfig(
            filename=log_file_name,
            format="%(asctime)s - %(filename)s - %(levelname)s - %(message)s",
        )
    else:
        logging.basicConfig()
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    if graylog_logger_address is not None:
        host, port = graylog_logger_address.split(":")
        handler = graypy.GELFTCPHandler(host, int(port), facility="ESS")
        logger.addHandler(handler)


def get_logger():
    return logging.LoggerAdapter(
        logging.getLogger(logger_name), {"process_name": f"{logger_name}"}
    )
