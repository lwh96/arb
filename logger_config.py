import logging
from logging.handlers import RotatingFileHandler
import sys
import os

def setup_logger(name="CryptoBot", log_file="bot_execution.log"):
    """
    Sets up a robust logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    # --- Formatters ---
    # File: Detailed (Time, Level, Module, Message)
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(message)s'
    )
    # Console: Cleaner
    console_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%H:%M:%S'
    )

    # --- Handler 1: Rotating File ---
    # Writes to bot_execution.log. 
    # When file hits 5MB, it renames it to bot_execution.log.1 and starts fresh.
    file_handler = RotatingFileHandler(
        log_file, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.INFO)

    # --- Handler 2: Console ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)

    # Add Handlers    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger