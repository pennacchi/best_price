import psycopg2
from psycopg2 import OperationalError
import os

class DBManager:
  """
  A class to manage the connection to a PostgreSQL database.
  """
  def __init__(self, dbname, user, password, host, port):
    self.dbname = dbname
    self.user = user
    self.password = password
    self.host = host
    self.port = port
    self.connection = None
    self.cursor = None

  def connect(self):
    """
    Establishes the connection to the database.
    Returns True if the connection is successful, False otherwise.
    """
    try:
        self.connection = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.cursor = self.connection.cursor()
        return True
    except OperationalError as e:
        return False
  
  def close(self):
      """
      Closes the cursor and the database connection, if they are open.
      """
      if self.cursor:
          self.cursor.close()
      if self.connection:
          self.connection.close()
  
  def execute_query(self, query, params=None):
      """
      Executes an SQL query (INSERT, UPDATE, DELETE).
      Returns True if the execution is successful, False otherwise.
      """
      if not self.connection:
          print("Não foi possível estabelecer uma conexão com o banco de dados.")
          return False
      
      try:
          if params:
              self.cursor.execute(query, params)
          else:
              self.cursor.execute(query)
          self.connection.commit()
          return True
      except Exception as e:
          self.connection.rollback()  # Rolls back the transaction in case of an error
          return False
  
  def fetch_all(self, query, params=None):
      """
      Executes a SELECT query and returns all results.
      """
      if not self.connection:
          return None
      
      try:
          if params:
              self.cursor.execute(query, params)
          else:
              self.cursor.execute(query)
          
          records = self.cursor.fetchall()
          return records
      except Exception as e:
          return None
  
if __name__ == "__main__":
    # Use example
    db_user = os.environ.get("DB_USER", "postgres")
    db_password = os.environ.get("DB_PASSWORD", "postgres")
    db_name = os.environ.get("DB_NAME", "my_database")

    db = DBManager(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host="localhost",
        port="5432"
    )

    if db.connect():
        # Example 1: Create table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100) UNIQUE
        );
        """
        db.execute_query(create_table_query)

        # Example 2: Inserto values
        insert_query = """
        INSERT INTO users (name, email) VALUES (%s, %s);
        """
        users_to_insert = [
            ('João', 'joao@example.com'),
            ('Maria', 'maria@example.com'),
            ('Carlos', 'carlos@example.com')
        ]

        for user_data in users_to_insert:
            db.execute_query(insert_query, user_data)

        # Example 3: Select
        select_query = "SELECT * FROM users;"
        users = db.fetch_all(select_query)

        if users:
            for user in users:
                print(user)

    # Example 4: Cleaning database
    # db.execute_query("DROP TABLE users;")

    db.close()
  