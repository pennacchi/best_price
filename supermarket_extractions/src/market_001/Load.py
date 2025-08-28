#! supermarket_extractions/venv/bin/python
import sys
from pathlib import Path
import json
from datetime import datetime

sys.path.append(str(Path.cwd() / 'supermarket_extractions'))

from src.best_price_db.Operations_ETL_Extract import Operations_ETL_Extract
from src.utils.logger import get_logger
from env.general import *


class Load:
  def __init__(self, logger):
    super().__init__()
    self.logger = logger
  
  def get_jsons_to_load(self):
    json_list = Operations_ETL_Extract().get_raw_json()
    return json_list

  def find_json(self, json_path, json_name):
    Path.exists(Path.cwd() / json_path / json_name)

  def run():
    logger = get_logger('market_001_load', PATH_LOGS)
    exec = Load(logger=logger)

    loading_start = datetime.now()

    for extraction_file in exec.get_jsons_to_load():
      json_path = extraction_file['source_path']
      json_name = extraction_file['source_file']
      extraction_id = extraction_file['id']
      relative_path = json_path + '/' + json_name
      
      logger.info(f'{" "*0}Checking file {json_name}...')

      if Path.exists(Path.cwd() / relative_path):
        logger.info(f'{" "*2}Loading...')

        with open(Path.cwd() / relative_path, 'r') as f:
          data = json.load(f)
          total_products = len(data)
          product_output = []

          for idx, product in enumerate(data):
            logger.info(f'{" "*4}Product {idx+1}/{total_products}...')
            logger.info(json.dumps(product,indent=4))
            
            product_output.append(
              (
                product['id'],
                product['name'],
                product['stock'],
                product['price'],
                product['sku'],
                product['brand'],
                product['name'],
                product['variableWeight'],
                product['store_id'],
                product['store'],
                product['category'],
                product['subcategory'],
                product['sub_subcategory'],
                extraction_id, # data come from this extraction
                loading_start, # transformed_at
              )
            )
      
      else:
        logger.info(f'{" "*2}File not found.')

if __name__ == '__main__':
  Load.run()
  

# Read all lines on operations.etl_extract where status is 'raw json'

# For each line

  # Try to find the json file

  # If exists

    # Read json file

    # Insert products in database table raw.market_001_product

    # Update status to 'loaded'

  # If not exists

    # Update status to 'json not found'