import logging.handlers
from pathlib import Path
import logging

def get_logger(filename, log_path):
  logger = logging.getLogger(__name__)
  logger.setLevel(logging.DEBUG)

  formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(funcName)s - %(message)s')

  # Store log path
  log_path.mkdir(exist_ok=True, parents=True)

  # Rotating File Log
  file_handler = logging.handlers.RotatingFileHandler(f'{log_path}/{filename}.log', mode='a', maxBytes=2E7, backupCount=3)
  file_handler.setFormatter(formatter)
  logger.addHandler(file_handler)

  # Stderr (print log)
  logger.addHandler(logging.StreamHandler()) # Print out on stdout

  return logger