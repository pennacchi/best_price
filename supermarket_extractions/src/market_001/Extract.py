#! supermarket_extractions/venv/bin/python
import sys
import json
from pathlib import Path
from bs4 import BeautifulSoup
from datetime import datetime

sys.path.append(str(Path.cwd() / 'supermarket_extractions'))

from src.utils.logger import get_logger
from src.utils.file_operations import store_file
from env.general import *
from src.core.APIManager import APIManager
from src.best_price_db.Operations_ETL_Extract import Operations_ETL_Extract

class Extract():
  def __init__(self, logger):
    self.config = self.get_config()
    self.api_manager = APIManager(logger=logger)
    self.logger = logger

  def get_config(self):
    with open(f'{PATH_ENV}/market_001.json') as f:
      return json.load(f)

  def get_sub_sub_categories(self):

    endpoint_config = self.config['endpoints']['categories']

    response = self.api_manager.request(
      url=endpoint_config['path'], 
      header=self.config['default_headers'], 
      method=endpoint_config['method'])
    
    soup = BeautifulSoup(response.text, 'html.parser')
    script = soup.find('script', id='__NEXT_DATA__')

    categories = json.loads(script.text)['props']['initialProps']['layoutProps']['categories']

    sub_sub_categories = []

    for category in categories:

      for subcategory in category['subCategory']:
        
        if len(subcategory['subCategory']) == 0 or len(subcategory['subCategory']) == 1:
          sub_sub_categories.append(subcategory['uiLink'].replace('/', '', 1).replace('/', '|'))

        if len(subcategory['subCategory']) > 1:
          for subsubcategory in subcategory['subCategory']:
            sub_sub_categories.append(subsubcategory['uiLink'].replace('/', '', 1).replace('/', '|'))

    return sub_sub_categories

  def get_products_total_pages_from_category(self, storeId, multicategory, resultsPerPage=100, page=1):
    endpoint_config = self.config['endpoints']['products_in_category']

    response = self.api_manager.request(
      url=self.config['base_url'] + endpoint_config['path'],
      header=self.config['default_headers'], 
      method=endpoint_config['method'],
      json={
        'partner': 'linx',
        'storeId': storeId,
        'multiCategory': multicategory,
        'resultsPerPage': resultsPerPage,
        'page': page,
        'sortBy': 'relevance',
        'department': 'ecom',
        'customerPlus': True
      },
      error_expected=True
      )
    
    if 'totalPages' not in response.json(): 
      return 0

    return response.json()['totalPages']

  def get_products_from_category(self, storeId, multicategory, resultsPerPage=100, page=1):
    
    endpoint_config = self.config['endpoints']['products_in_category']
    
    response = self.api_manager.request(
      url=self.config['base_url'] + endpoint_config['path'],
      header=self.config['default_headers'], 
      method=endpoint_config['method'],
      json={
        'partner': 'linx',
        'storeId': storeId,
        'multiCategory': multicategory,
        'resultsPerPage': resultsPerPage,
        'page': page,
        'sortBy': 'relevance',
        'department': 'ecom',
        'customerPlus': True
      },
      )
    
    return response.json()['products']

  def run():
    logger = get_logger('market_001_product_extraction', PATH_LOGS)
    logger.info('Starting extraction from Market 001...')

    extraction_start = datetime.now()

    product_market = Extract(logger)
    
    stores = product_market.config['stores']
    
    product_final = []


    for store in stores:
      storeId = store['storeId']
      store_name = store['store']
      if not store['active']:
        continue

      logger.info(f'{" "*2}Extracting products from {store_name} ({storeId})...')

      sub_sub_categories = product_market.get_sub_sub_categories()

      for c_idx, sub_sub_category in enumerate(sub_sub_categories):
        

        logger.info(f'{" "*4}Extracting products from sub_subcategory {sub_sub_category} ({c_idx+1}/{len(sub_sub_categories)})')

        current_page = 1

        total_pages = product_market.get_products_total_pages_from_category(storeId, sub_sub_category, page=current_page)

        if total_pages == 0:
          logger.info(f'{" "*6}0 products found in sub_subcategory {sub_sub_category}')
          continue

        while current_page <= total_pages:

          logger.info(f'{" "*6}Extracting products from page {current_page}/{total_pages}')
          products = product_market.get_products_from_category(storeId, sub_sub_category, page=current_page)

          for product in products:
            product['*store_id'] = storeId
            product['*store'] = store_name
            product['*category'] = sub_sub_category.split('|')[0]

            if len(sub_sub_category.split('|')) >= 2:
              product['*subcategory'] = sub_sub_category.split('|')[1]
            else:
              product['*subcategory'] = ''
            
            if len(sub_sub_category.split('|')) >= 3:
              product['*sub_subcategory'] = sub_sub_category.split('|')[2]
            else:
              product['*sub_subcategory'] = ''

          product_final += products
          current_page += 1
          total_pages = product_market.get_products_total_pages_from_category(storeId, sub_sub_category, page=current_page)
        # while current_page <= total_pages:
      # for c_idx, sub_sub_category in enumerate(sub_sub_categories):
    # for store in stores:

    filename = f'products_{datetime.now().strftime("%Y_%m_%d %H_%M_%S")}.json'

    store_file(
      PATH_RAW_MARKET_001,
      filename,
      json.dumps(product_final, indent=4)
    )

    Operations_ETL_Extract(logger).insert(
      market='market_001',
      source_path=str(PATH_RAW_MARKET_001).replace(f'{Path.cwd()}/', ''),
      source_file=filename,
      extraction_start=extraction_start,
      extraction_end=datetime.now(),
      status='stored as raw json'
    )
    logger.info('Extraction completed!')


if __name__ == "__main__":
  ## Complete run
  Extract.run()

  ## Test each function
  # execution = Extract(get_logger('market_001_product_extraction', PATH_LOGS))
  # result = execution.get_sub_sub_categories()
  # result = execution.get_products_total_pages_from_category(2232, 'alimentos|acougue', 100, 1)
  # result = execution.get_products_from_category(2232, 'alimentos|acougue', 100, 1)
  # print(result)