#! supermarket_extractions/venv/bin/python
import sys
from pathlib import Path

sys.path.append(str(Path.cwd() / 'supermarket_extractions'))

from src.best_price_db.Connection import Connection

class Operations_ETL_Extract(Connection):
  def __init__(self):
    super().__init__()
  

  def insert(self, market, source_path, source_file, extraction_start, extraction_end, status):

    cursor = Connection()
    create_schema = cursor.execute_query("CREATE SCHEMA operation;")
    if create_schema:
      self.logger.info('Schema "operation" was created.')
    
    create_table = cursor.execute_query(""" 
    CREATE TABLE IF NOT EXISTS operation.etl_extract (
      id serial PRIMARY KEY,
      market VARCHAR(255),
      source_path VARCHAR(255),
      source_file VARCHAR(255),
      extraction_start TIMESTAMP,
      extraction_end TIMESTAMP,
      status VARCHAR(255)
    ) """)

    if create_table:
      self.logger.info('Table "operation.etl_extract" was created.')

    cursor.execute_query(f"""
      INSERT INTO operation.etl_extract (
        market,
        source_path,
        source_file,
        extraction_start,
        extraction_end,
        status
      ) VALUES (
        '{market}',
        '{source_path}',
        '{source_file}',
        '{extraction_start}',
        '{extraction_end}',
        '{status}'
      )
      """)
  

  def get_raw_json(self):
    cursor = Connection()
    result = cursor.fetch_all("""
      SELECT 
        market,
        source_path,
        source_file,
        extraction_start,
        extraction_end,
        status
      FROM operation.etl_extract WHERE status = 'stored as raw json'
    """)
    result_objects = []
    for r in result:
      result_objects.append({
        'market': r[0],
        'source_path': r[1],
        'source_file': r[2],
        'extraction_start': r[3],
        'extraction_end': r[4],
        'status': r[5]
      })
    cursor.close()
    return result_objects

if __name__ == '__main__':
  cursor = Operations_ETL_Extract()
  result = cursor.get_raw_json()
  print(result)
  cursor.close()