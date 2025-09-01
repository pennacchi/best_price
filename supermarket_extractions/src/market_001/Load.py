#! supermarket_extractions/venv/bin/python
import sys
from pathlib import Path
import json
from datetime import datetime

sys.path.append(str(Path.cwd() / 'supermarket_extractions'))

from src.best_price_db.Operation_ETL_Extract import Operation_ETL_Extract
from src.best_price_db.Raw_Products_Market_001 import Raw_Products_Market_001
from src.utils.logger import get_logger
from env.general import *


class Load:
  def __init__(self, logger):
    super().__init__()
    self.logger = logger
  
  def get_jsons_to_load(self):
    with Operation_ETL_Extract() as etl_extract:
      operation_etl_extract_list = etl_extract.get_raw_json()

    return operation_etl_extract_list

  def find_json(self, json_path, json_name):
    Path.exists(Path.cwd() / json_path / json_name)

  def prepare_product_tuples(self, product_list, product, extraction_id, loading_start):
    if len(product['productImages']) > 0:
      product_img_url = product['productImages'][0]
    else:
      product_img_url = ''
    
    if 'brand' in product.keys():
      brand = product['brand']
    else:
      brand = ''

    product_list.append(
      (
        product['id'],
        product['name'],
        product['stock'],
        product['price'],
        product['sku'],
        brand,
        product_img_url,
        product['urlDetails'],
        product['*store_id'],
        product['*store'],
        product['*category'],
        product['*subcategory'],
        product['*sub_subcategory'],
        extraction_id, # data come from this extraction
        loading_start, # transformed_at
      )
    )
    return product_list

  def run():
    logger = get_logger('market_001_load', PATH_LOGS)
    exec = Load(logger=logger)
    
    logger.info(f'{" "*0}Starting loading process...')
    loading_start = datetime.now()

    for operation_etl_extract in exec.get_jsons_to_load():
      json_path = operation_etl_extract['source_path']
      json_name = operation_etl_extract['source_file']
      extraction_id = operation_etl_extract['id']
      relative_path = json_path + '/' + json_name
      
      logger.info(f'{" "*0}Checking file {json_name}...')

      if Path.exists(Path.cwd() / relative_path):
        logger.info(f'{" "*2}File found...')

        with open(Path.cwd() / relative_path, 'r') as f:
          data = json.load(f)
          total_products = len(data)
          product_output = []

          logger.info(f'{" "*2}Preparating {total_products} products...')
          for idx, product in enumerate(data):
            exec.prepare_product_tuples(product_output, product, extraction_id, loading_start)
        
        logger.info(f'{" "*2}Inserting to database...')

        with Raw_Products_Market_001() as raw_products_db:
          raw_products_db.insert_products(product_output)

        logger.info(f'{" "*2}Updating ETL_Operation status...')

        with Operation_ETL_Extract() as etl_extract:
           etl_extract.update_status(extraction_id, 'json loaded')

      if not Path.exists(Path.cwd() / relative_path):

        logger.info(f'{" "*2}File  not found...')
        with Operation_ETL_Extract() as etl_extract:
           etl_extract.update_status(extraction_id, 'json not found')

if __name__ == '__main__':
  Load.run()