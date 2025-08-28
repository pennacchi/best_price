#! supermarket_extractions/venv/bin/python
import sys
from pathlib import Path
import yaml

sys.path.append(str(Path.cwd() / 'supermarket_extractions'))
from src.core.DBManager import DBManager
from env.general import *

class Connection(DBManager):
  def __init__(self):
    self.config = self.get_config()
    super().__init__(
      dbname=self.config['db'], 
      user=self.config['user'], 
      password=self.config['password'], 
      host=self.config['host'], 
      port=self.config['port']
    )
    self.connect()
  
  def get_config(self):
    with open(PATH_ENV / 'config.yaml') as f:
      config = yaml.safe_load(f)
      environemnt_selected = config['environemnt_selected']
      return config['environemnt_targets'][environemnt_selected]['best_price_db']

if __name__ == '__main__':
  cursor = Connection()
  result = cursor.fetch_all("SELECT datname FROM pg_database;")
  cursor.close()