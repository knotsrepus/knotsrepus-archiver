import logging


def get_logger(name):
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(name)s:%(lineno)d] %(levelname)s: %(message)s"))

    logger = logging.Logger(name, logging.INFO)
    logger.addHandler(handler)

    return logger


def log_response(response, logger):
    if response.ok:
        logger.info(f"{response.url} {response.status} {response.reason}")
    else:
        logger.warning(f"{response.url} {response.status} {response.reason}")
