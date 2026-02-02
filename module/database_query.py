#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pymysql
from module.log import Log

logging = Log(__name__).getlog()

class DatabaseHandler:

    def __init__(self, mysql_info):
        self.host = mysql_info['host']
        self.user = mysql_info['user']
        self.password = mysql_info['password']
        self.database = mysql_info['database']
        self.port = mysql_info['port']
        self.conn = None
        self.cursor = None

    def connect_db(self):
        try:
            self.conn = pymysql.connect(
                host=self.host, 
                user=self.user, 
                password=self.password, 
                database=self.database, 
                port=self.port
            )
            self.cursor = self.conn.cursor()
            logging.debug("Connected to MySQL database")
        except pymysql.MySQLError as e:
            logging.error("Failed to connect to MySQL database: %s", e)
            raise

    def disconnect_db(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logging.debug("Disconnected from MySQL database")

    def fetch_data(self, sql_query):
        try:
            logging.debug("Executing MySQL query: %s", sql_query)
            self.cursor.execute(sql_query)
            result = self.cursor.fetchall()  # Fetch all rows
            logging.debug("Query returned %d rows", len(result))
            return result
        except pymysql.MySQLError as e:
            logging.error("Failed to execute MySQL query: %s", e)
            raise

    def is_single_row(self, sql_query):
        try:
            logging.debug("Executing MySQL query: %s", sql_query)
            self.cursor.execute(sql_query)
            result = self.cursor.fetchall()  # Fetch all rows
            row_count = len(result)
            logging.debug("Query returned %d rows", row_count)
            if row_count == 1:
                return True, result[0]
            elif row_count > 1:
                return False, result
            else:
                return False, None
        except pymysql.MySQLError as e:
            logging.error("Failed to execute MySQL query: %s", e)
            raise
        
    def insert_data(self, sql):
        """
        插入数据到数据库表中
        """
        try:

            if isinstance(sql, list):
                # 插入多行数据
                self.cursor.executemany(sql)
            else:
                # 插入单行数据
                self.cursor.execute(sql)
            
            self.conn.commit()
            logging.debug("Data inserted successfully into : %s", sql)
        except pymysql.MySQLError as e:
            logging.error("Failed to insert data into MySQL table: %s", e)
            self.conn.rollback()
            raise
    
    def delete_data(self, sql):
        """
        删除数据库表中的数据
        """
        try:
            logging.debug("Executing MySQL query: %s", sql)
            self.cursor.execute(sql)
            self.conn.commit()
            logging.debug("Data deleted successfully: %s", sql)
        except pymysql.MySQLError as e:
            logging.error("Failed to delete data from MySQL table: %s", e)
            self.conn.rollback()
            raise
 
# 使用示例
# if __name__ == "__main__":
#     db_handler = DatabaseHandler(
#         host='192.168.12.51', 
#         user='root', 
#         password='Root@123!@#', 
#         database='pts_oam_center_demo_2.1.0', 
#         port=3306
#     )
#     db_handler.connect_db()
#     query = "SELECT id FROM node_line WHERE node_line_name = 'N201互联网'"
    
#     # 判断查询结果是否为单行
#     is_single, data = db_handler.is_single_row(query)
#     if is_single:
#         logging.info("Query returned a single row: %s", data)
#     elif data:
#         logging.info("Query returned multiple rows: %s", data)
#     else:
#         logging.info("Query returned no rows")
    
#     db_handler.disconnect_db()