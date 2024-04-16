import logging
import configparser
import os

from datetime import datetime

# # config data 가져오기
config = configparser.ConfigParser()
config.read('../config/config.ini', encoding="UTF-8")
logger_level = config['LOG']['file_log_level']
console_level = config['LOG']['console_log_level']


def get_logger(file_name="collect_log", filemode="a"):
    logger = logging.getLogger(file_name)
    logger.setLevel(logging.DEBUG)
    # logger.propagate = False

    # Check handler exists
    if len(logger.handlers) > 0:
        return logger

    log_file_path = config['LOG']['log_file_path']
    log_file_full_name = log_file_path + '/' + file_name + '_' + str(datetime.now().date()).replace('-', '') + ".log"
    log_format = "[%(levelname)s] %(asctime)s (%(filename)s)[line : %(lineno)d] - %(message)s"

    if logger_level == 'DEBUG':
        log_level = logging.DEBUG
    elif logger_level == 'INFO':
        log_level = logging.INFO
    elif logger_level == 'ERROR':
        log_level = logging.ERROR
    else:
        log_level = logging.INFO

    if console_level == 'DEBUG':
        console_log_level = logging.DEBUG
    elif console_level == 'INFO':
        console_log_level = logging.INFO
    elif console_level == 'ERROR':
        console_log_level = logging.ERROR
    else:
        console_log_level = logging.INFO

    try:
        if not os.path.exists(log_file_path):
            os.makedirs(log_file_path)

        console = logging.StreamHandler()
        console.setLevel(console_log_level)
        console.setFormatter(logging.Formatter(log_format))

        file_handler = logging.FileHandler(filename=log_file_full_name)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(logging.Formatter(log_format))

        logger.addHandler(console)
        logger.addHandler(file_handler)

        # logging.basicConfig(filename=log_file_full_name,
        #                     filemode=filemode,
        #                     format=log_format,
        #                     level=logging.DEBUG)

    except OSError as e:
        return e
    else:
        logger.info(f"log level is {logger.level}")
        return logger
