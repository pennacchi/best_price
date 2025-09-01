#! supermarket_extractions/venv/bin/python
from multiprocessing.spawn import prepare
import sys
from pathlib import Path

sys.path.append(str(Path.cwd() / 'supermarket_extractions'))

from src.best_price_db.Connection import Connection

class Raw_Products_Market_001(Connection):
  def __init__(self):
    super().__init__()
    self.create_table()
  
  def create_table(self):
    self.execute_query("CREATE SCHEMA IF NOT EXISTS raw;")
    
    self.execute_query(""" 
    CREATE TABLE IF NOT EXISTS raw.products_market_001 (
      id serial PRIMARY KEY,
      id_market int,
      name VARCHAR(255),
      stock boolean,
      price float,
      sku VARCHAR(255),
      brand VARCHAR(255),
      product_img_url VARCHAR(255),
      url VARCHAR(255),
      store_id int,
      store VARCHAR(255),
      category VARCHAR(255),
      subcategory VARCHAR(255),
      sub_subcategory VARCHAR(255),
      extraction_id int,
      transformed_at TIMESTAMP
    ) """)
  
  def insert_products(self, product_list):
    
    self.create_table()

    insert_query = """
        INSERT INTO raw.products_market_001 (
          id_market,
          name,
          stock,
          price,
          sku,
          brand,
          product_img_url,
          url,
          store_id,
          store,
          category,
          subcategory,
          sub_subcategory,
          extraction_id,
          transformed_at
        ) VALUES 
      """
    query_args = [self.cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row).decode('utf-8') for row in product_list]
    query_args = ','.join(query_args)
    query = insert_query + query_args
    self.execute_query(query)


if __name__ == '__main__':
  with Raw_Products_Market_001() as raw_products_market_001:
    # raw_products_market_001.create_table()
    raw_products_market_001.insert_products([
      (
        1,
        'Product 1',
        True,
        10.99,
        'SKU-001',
        'Brand 1',
        'https://example.com/product1.jpg',
        'https://example.com/product1',
        1,
        'Store 1',
        'Category 1',
        'Subcategory 1',
        'Sub-subcategory 1',
        1,
        '2023-01-01 00:00:00'
      )
      ,
      (
        2,
        'Product 2',
        False,
        19.99,
        'SKU-002',
        'Brand 2',
        'https://example.com/product2.jpg',
        'https://example.com/product2',
        2,
        'Store 2',
        'Category 2',
        'Subcategory 2',
        'Sub-subcategory 2',
        1,
        '2023-01-01 00:00:00'
      )
    ])