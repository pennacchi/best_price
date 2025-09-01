#! supermarket_extractions/venv/bin/python
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

sys.path.append(str(Path.cwd() / 'supermarket_extractions'))

from src.best_price_db.Connection import Connection
from env.general import *

class Operation_ETL_Extract(Connection):
  def __init__(self):
    super().__init__()
  
  def create_table(self):
    self.execute_query("CREATE SCHEMA IF NOT EXISTS operation;")
    
    self.execute_query(""" 
      CREATE TABLE IF NOT EXISTS operation.etl_extract (
        id serial PRIMARY KEY,
        market VARCHAR(255),
        source_path VARCHAR(255),
        source_file VARCHAR(255),
        extraction_start TIMESTAMP,
        extraction_end TIMESTAMP,
        status VARCHAR(255)
      ) """)

    

  def insert(self, market, source_path, source_file, extraction_start, extraction_end, status):

    self.create_table()

    query = """
      INSERT INTO operation.etl_extract (
        market,
        source_path,
        source_file,
        extraction_start,
        extraction_end,
        status
      ) VALUES (
        %s,
        %s,
        %s,
        %s,
        %s,
        %s
      )
    """
    agrs = (market, str(source_path), str(source_file), extraction_start, extraction_end, status)
    final_query = self.cursor.mogrify(query, agrs)
    self.execute_query(final_query.decode('utf-8'))

  
  def get_all(self):
    result = self.fetch_all("""
      SELECT 
        id,
        market,
        source_path,
        source_file,
        extraction_start,
        extraction_end,
        status
      FROM operation.etl_extract
    """)
    result_objects = []
    for r in result:
      result_objects.append({
        'id': r[0],
        'market': r[1],
        'source_path': r[2],
        'source_file': r[3],
        'extraction_start': r[4],
        'extraction_end': r[5],
        'status': r[6]
      })
    return result_objects

  def get_raw_json(self):
    
    result = self.fetch_all("""
      SELECT 
        id,
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
        'id': r[0],
        'market': r[1],
        'source_path': r[2],
        'source_file': r[3],
        'extraction_start': r[4],
        'extraction_end': r[5],
        'status': r[6]
      })
    return result_objects

  def update_status(self, id, status):
    self.execute_query(f"""
      UPDATE operation.etl_extract SET status = '{status}' WHERE id = {id}
    """)
  
  def update_not_found_json_files(self):
    """
      This function will add new rows to operation.etl_extract
      looking for json files that are on the folder, but are not on the database
    """
    extracts_not_loaded = self.get_raw_json()
    for extract in extracts_not_loaded:
      json_path = extract['source_path']
      json_name = extract['source_file']
      relative_path = json_path + '/' + json_name
      if not Path.exists(Path.cwd() / relative_path):
        self.update_status(extract['id'], 'json not found')
        print(f'Json {json_name} not found')
      else:
        print(f'Json {json_name} found')
    
    return
  
  def insert_new_json_files_to_table(self):
    """
      This function will add new rows to operation.etl_extract
      looking for json files that are on the folder, but are not on the database
    """
    extracts_all = self.get_all()
    
    for file in PATH_RAW_MARKET_001.iterdir():
      if file.name not in [extract['source_file'] for extract in extracts_all]:

        now_time = datetime.now(timezone(timedelta(hours=-3)))
        

        self.insert(
          market='market_001',
          source_path=PATH_RAW_MARKET_001,
          source_file=file.name,
          extraction_start=now_time,
          extraction_end=None,
          status='stored as raw json'
        )
        print(f'Json {file.name} not found')
      else:
        print(f'Json {file.name} found')
    return

  def adjust_missing_json_files(self):
    """
      This function will add new rows to operation.etl_extract
      looking for json files that are on the folder, but are not on the database
      It will also verify rows of our operation.etl_extract that don't exist
      on the folder 
    """
    self.update_not_found_json_files()
    self.insert_new_json_files_to_table()
    return

  

if __name__ == '__main__':
  # # get all raw json
  # with Operation_ETL_Extract() as etl_extract:
  #   result = etl_extract.get_raw_json()
  # print(result)
  
  ## Insert example
  # with Operation_ETL_Extract() as etl_extract:
  #   etl_extract.insert('market_001', 'path', 'file', '2023-01-01 00:00:00', '2023-01-01 00:00:00', 'stored as raw json')
  
  # with Operation_ETL_Extract() as etl_extract:
  #   etl_extract.adjust_missing_json_files()
  pass