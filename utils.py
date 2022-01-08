import datetime
import os
import sys
import logging
import hashlib
from logging import StreamHandler, Formatter


def get_logger(name):
    """
    Shortcut for getting debug logger
    :param name: str; name of logger
    :return: logging.Logger; Logger of logging.DEBUG level
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    stream_handler = StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(Formatter(fmt='[%(name)s: %(asctime)s: %(levelname)s] %(message)s'))
    logger.addHandler(stream_handler)

    if 'log' not in os.listdir():
        os.mkdir('log')

    file_handler = logging.FileHandler(f'log/{datetime.date.today()}.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(Formatter(fmt='[%(name)s: %(asctime)s: %(levelname)s] %(message)s'))
    logger.addHandler(file_handler)

    return logger


def sha256(string, size):
    """
    Returns last <size> figures of sha256 hash of string encoded as utf-8, digested as hex
    """

    return int(hashlib.sha256(string.encode('utf-8')).hexdigest(), 16) % 10 ** size
