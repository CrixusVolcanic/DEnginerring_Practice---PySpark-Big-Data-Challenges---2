import logging

def setup_logger(name, log_file, level=logging.INFO):

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    logger = logging.getLogger(name)
    logger.setLevel(level)

    for handler in [logging.FileHandler(log_file), logging.StreamHandler()]:
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

defaul_logger = setup_logger(name="app", log_file="default.log")

def format_column(column_name):
    return str(column_name).lower().replace("-", '_').replace(" ", "_")