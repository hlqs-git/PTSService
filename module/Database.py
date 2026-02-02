import mysql.connector
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class DatabaseHandler:
    def __init__(self, mysql_info):
        self.mysql_info = mysql_info
        self.conn = None
        self.cursor = None

    def connect_db(self):
        try:
            self.conn = mysql.connector.connect(**self.mysql_info)
            self.cursor = self.conn.cursor(buffered=True)
            logging.info("Connected to MySQL database")
        except mysql.connector.Error as err:
            logging.error(f"Error connecting to MySQL: {err}")
            raise

    def disconnect_db(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
                logging.info("Disconnected from MySQL database")
        except mysql.connector.Error as err:
            logging.error(f"Error disconnecting from MySQL: {err}")
            raise

    def execute_mysql(self, execute_sql, params=None):
        try:
            self.cursor.execute(execute_sql, params)
        except mysql.connector.Error as err:
            logging.error(f"Error executing query: {err}")
            raise

    def insert_data(self, insert_sql, params=None):
        try:
            self.execute_mysql(insert_sql, params)
            self.conn.commit()
        except mysql.connector.Error as err:
            logging.error(f"Error inserting data into MySQL: {err}")
            self.conn.rollback()
            raise

    def insert_data_many(self, insert_sql, params_list):
        try:
            self.cursor.executemany(insert_sql, params_list)
            self.conn.commit()
        except mysql.connector.Error as err:
            logging.error(f"Error inserting multiple data into MySQL: {err}")
            self.conn.rollback()
            raise

    def delete_data(self, delete_sql, params=None):
        try:
            self.execute_mysql(delete_sql, params)
            self.conn.commit()
        except mysql.connector.Error as err:
            logging.error(f"Error deleting data from MySQL: {err}")
            self.conn.rollback()
            raise

    def start_transaction(self):
        try:
            self.conn.start_transaction()
        except mysql.connector.Error as err:
            logging.error(f"Error starting transaction in MySQL: {err}")
            raise

    def commit(self):
        try:
            self.conn.commit()
        except mysql.connector.Error as err:
            logging.error(f"Error committing transaction in MySQL: {err}")
            raise

    def rollback(self):
        try:
            self.conn.rollback()
        except mysql.connector.Error as err:
            logging.error(f"Error rolling back transaction in MySQL: {err}")
            raise
