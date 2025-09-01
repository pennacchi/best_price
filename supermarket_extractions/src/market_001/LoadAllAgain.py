#! supermarket_extractions/venv/bin/python
import sys
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path.cwd() / 'supermarket_extractions'))

from src.best_price_db.Operation_ETL_Extract import Operation_ETL_Extract
from src.best_price_db.Raw_Products_Market_001 import Raw_Products_Market_001
from src.market_001.Load import Load as Load_Products
from src.utils.logger import get_logger
from env.general import *
import time

class LoadAllAgain:
  def __init__(self):
    self.logger = get_logger('market_001_process_all_again', PATH_LOGS)

  def alert(self):
    self.logger.info(f'{" "*0}!!! CAUTION !!!')
    time.sleep(0.5)
    self.logger.info(f'{" "*0}!!! CAUTION !!!')
    time.sleep(0.5)
    self.logger.info(f'{" "*0}!!! CAUTION !!!\n')
    time.sleep(1)
    self.logger.info('This script will delete all products from market_001 database.')
    time.sleep(3)
    self.logger.info('Are you sure?\n')
    time.sleep(1)
    self.logger.info('Write "yes" to confirm:')

    confirmation = input()

    if confirmation.lower() != 'yes':
      self.logger.info('\nAborting script execution.\n')
      exit()
    self.logger.info('\nContinuing script execution.\n')
  
  def delete_products(self):
    with Raw_Products_Market_001() as raw_products_db:
      raw_products_db.execute_query("DELETE FROM raw.products_market_001;")
  
  def mark_all_etl_extract_as_to_process_again(self):
    with Operation_ETL_Extract() as operation_etl_extract:
      operation_etl_extract.execute_query("""
        UPDATE operation.etl_extract SET status = 'stored as raw json' where market = 'market_001';
      """)
  
  def load_all_products(self):
    load_products = Load_Products.run()

  def run(self):
    self.alert()

    self.logger.info(f'{" "*0}Deleting all products from market_001 database...')
    self.delete_products()
    self.logger.info(f'{" "*0}Marking all etl_extract as to_process_again...')
    self.mark_all_etl_extract_as_to_process_again()
    self.logger.info(f'{" "*0}Loading all products from all jsons...')
    self.load_all_products()



if __name__ == '__main__':
  LoadAllAgain().run()