#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime
import openpyxl
import json
import os
from module.log import Log
from module.database_query import DatabaseHandler

logging = Log(__file__).getlog()

home_dire = os.path.dirname(os.path.abspath(__file__))

class LogicalNodeLine(object):
    def __init__(self, filename, mysql_info):
        self.filename = filename
        self.mysql_info = mysql_info
        self.db_handler = DatabaseHandler(self.mysql_info)
        self.db_handler.connect_db()
        self.directory = f'{os.path.dirname(home_dire)}/traffic/logical_node_line'
        self.Physical_node_line_directory = f'{os.path.dirname(home_dire)}/traffic/physical_node_line'
        
    def get_logical_node_line_sheet(self):
        
        def inquire_node_line_id(node_line_name):
            query = f"SELECT id FROM node_line WHERE node_line_name = '{node_line_name}'"
            
            is_single, data = self.db_handler.is_single_row(query)
            if is_single:
                logging.debug(f"{node_line_name} Query returned a single row: %s", data)
                return data[0]
            elif data:
                logging.error("Query returned multiple rows: %s", data)
                raise Exception(f"{node_line_name} Query returned multiple rows")
            else:
                logging.error(f"{node_line_name} Query returned no rows")
                raise Exception(f"{node_line_name} Query returned no rows")

            
        def get_file(l_name, p_name, l_id, p_id):
            all_files = os.listdir(self.Physical_node_line_directory)
            logging.debug(f'all_files: {all_files}')
            
            files = [f for f in all_files if os.path.isfile(os.path.join(self.Physical_node_line_directory, f))]
            logging.debug(f'files: {files}')
            if p_name in files:
                p_file = os.path.join(self.Physical_node_line_directory, p_name)
                logging.debug(f'p_file: {p_file}')
                with open(p_file, 'r') as f:
                    p_data = json.load(f)
                    
                    # 使用一个临时变量来保存更新后的数据
                    temp_data = {}
                    for keys, values in list(p_data[p_name].items()):  # 使用list()创建副本
                        if values['node_line_id'] == int(p_id):
                            temp_data[keys] = values
                            temp_data[keys]['node_line_id'] = int(l_id)
                        else:
                            logging.error(f'文件中物理节点线路id: {values["node_line_id"]} 与数据库中:{p_id} 不一致')
                            raise Exception(f'文件中物理节点线路id: {values["node_line_id"]} 与数据库中:{p_id} 不一致')
                    
                    # 更新原始字典
                    p_data[l_name] = temp_data
                    del p_data[p_name]
                    
                    with open(f'{self.directory}/{l_name}', 'w', encoding='utf-8') as f:
                        json.dump(p_data, f, ensure_ascii=False, indent=4)
                    
                    logging.info(f'{l_name} has been saved!')
            else:
                logging.error(f'{p_name} 文件不存在')
                raise Exception(f'{p_name} 文件不存在')
            
        
        wb = openpyxl.load_workbook(self.filename)
        sheet = wb['逻辑节点线路']
        
        headers = {cell.value: cell.column - 1 for cell in sheet[1]}
        
        for row in sheet.iter_rows(min_row=2, values_only=True):
            logical_node_line_name = row[headers['逻辑节点选路名称']]
            physical_node_line_name = row[headers['对应物理线路']]
            logging.debug(f'逻辑节点线路名称: {logical_node_line_name}')
            logging.debug(f'带宽最小物理节点线路名称: {physical_node_line_name}')
            
            logical_node_line_id = inquire_node_line_id(logical_node_line_name)
            physical_node_line_id = inquire_node_line_id(physical_node_line_name)
            logging.debug(f'逻辑节点线路id: {logical_node_line_id}')
            logging.debug(f'带宽最小物理节点线路id: {physical_node_line_id}')
            
            get_file(logical_node_line_name, physical_node_line_name, 
                     logical_node_line_id, physical_node_line_id)
        
        self.db_handler.disconnect_db()
            


class LNodeLineInsertDatabase(object):
    def __init__(self, mysql_info):
        self.mysql_info = mysql_info
        self.db_handler = DatabaseHandler(self.mysql_info)
        self.db_handler.connect_db()
        self.directory = f'{os.path.dirname(home_dire)}/traffic/logical_node_line'
        
    def inter_database_traffic(self, sql):
        self.db_handler.insert_data(sql)

    def delete_database_traffic(self, sql):
        self.db_handler.delete_data(sql)
        
    def get_file(self):
        
        def get_formatted_date_from_timestamp(timestamp):
            # 将时间戳转换为datetime对象
            dt_object = datetime.datetime.fromtimestamp(timestamp)
            
            # 格式化日期为YYYYMM
            formatted_date = dt_object.strftime('%Y%m')
            
            return formatted_date
        
        all_files = os.listdir(self.directory)
        files = [f for f in all_files if os.path.isfile(os.path.join(self.directory, f))]
        for file in files:

            logging.debug(f'open file: {file}')
            with open(f'{self.directory}/{file}', 'r') as f:
                node_line_data_dict = json.load(f)
                node_line_data_keys = list(node_line_data_dict.keys())

                node_line_data_keys_list = node_line_data_dict[node_line_data_keys[0]].keys()

                # node_line_data_keys_list = list(node_line_data_dict[node_line_data_keys].keys())

                for key in node_line_data_keys_list:
                    # print(key)
                    formatted_date = get_formatted_date_from_timestamp(int(key) / 1000)
                    
                    node_line_id = node_line_data_dict[node_line_data_keys[0]][key]["node_line_id"]
                    tx_traffic = node_line_data_dict[node_line_data_keys[0]][key]["tx_traffic"]
                    rx_traffic = node_line_data_dict[node_line_data_keys[0]][key]["rx_traffic"]

                    
                    query = f'INSERT into node_line_traffic_{formatted_date}(node_line_id,data_time,tx_traffic,rx_traffic) VALUES ({node_line_id},{key},"{tx_traffic}","{rx_traffic}");'
                    delete_sql = f'DELETE FROM node_line_traffic_{formatted_date} WHERE node_line_id = {node_line_id} AND data_time = {key};'

                    # 开启多线程数据插入会出现失败
                    # thread = threading.Thread(target=self.inter_database_traffic, args=(query,))
                    # thread.start()   
                    
                    logging.debug(f'delete_sql: {delete_sql}')
                    self.delete_database_traffic(delete_sql)
                    
                    logging.debug(f'insert: {query}')
                    self.inter_database_traffic(query)
                
                logging.info(f'Logical_node_line file: {file} insert success')
        
        self.db_handler.disconnect_db()

            

            
        
        
