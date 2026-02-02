#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import openpyxl
import threading
import datetime
import os
import re
import random
import json
from module.log import Log
from module.database_query import DatabaseHandler
from module.traffice import TrafficFlow

logging = Log(__name__).getlog()
home_dire = os.path.dirname(os.path.abspath(__file__))

class PhysicalNodeLine(object):
    def __init__(self, filename, mysql_info):
        self.filename = filename
        self.mysql_info = mysql_info
        self.directory = home_dire

    def save_data(self, data):
        node_name_dict_keys = data.keys()
        node_name = list(node_name_dict_keys)[0]
        
        traffic_files_path = f'{os.path.dirname(self.directory)}/traffic/physical_node_line/'
        all_files = os.listdir(traffic_files_path)
        files = [f for f in all_files if os.path.isfile(os.path.join(traffic_files_path, f))]
        
        if node_name in files:
            with open(f'{traffic_files_path}/{node_name}', 'r', encoding='utf-8') as f:
                data_old = json.load(f)
                
            data_old[node_name].update(data[node_name])

            with open(f'{traffic_files_path}/{node_name}', 'w', encoding='utf-8') as f:
                json.dump(data_old, f, ensure_ascii=False, indent=4)
                
            logging.info(f'{node_name} has been updated')
        else:
            with open(f'{traffic_files_path}/{node_name}', 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                
            logging.info(f'{node_name} has been saved')
        
    def thread_run(self,data_file, file_num, bandwidth, start_time, end_time, node_name, num):
        trafficeflow = TrafficFlow(data_file, int(file_num), int(bandwidth), int(start_time), int(end_time), node_name, int(num))
        trafficeflow_all = trafficeflow.get_traffic_flow()
        trafficeflow_gen = trafficeflow.generate_traffic()
        # print(trafficeflow_gen)
        self.save_data(trafficeflow_gen)
        
        
    def get_random_file(self):
        
        # 获取当前文件目录父目录
        directory = os.path.dirname(self.directory)
        logging.debug(f'directory: {directory}')
        
        # 获取目录下的所有文件和文件夹
        all_files = os.listdir(f'{directory}/template')
        
        # 过滤掉文件夹，只保留文件
        files = [f for f in all_files if os.path.isfile(os.path.join(f'{directory}/template', f))]
        logging.debug(files)
        
        # 如果目录中没有文件，返回 None
        if not files:
            return None, None
        
        # 随机选择一个文件
        data_file = random.choice(files)
        logging.debug(f'data_file: {data_file}')
        
        # 使用正则表达式查找字符串中的数字
        match = re.search(r'(\d+)', data_file)
        if match:
            return f'{directory}/template/{data_file}', match.group(1)
        else:
            return data_file, None
        
    def get_physical_node_line_sheet(self):
        wb = openpyxl.load_workbook(self.filename, data_only=True)
        sheet = wb['物理节点线路']
        
        # 获取标题行
        headers = {cell.value: cell.column - 1 for cell in sheet[1]}
        
        # 循环获取数据
        for row in sheet.iter_rows(min_row=2, values_only=True):
            node_name = row[headers['节点线路名称']]
            logging.debug(f'node_name: {node_name}')
            
            bandwidth = row[headers['带宽(M)']]
            logging.debug(f'bandwidth: {bandwidth}')
            
            start_time = row[headers['开始时间']]
            logging.debug(f'start_time: {start_time}')
            
            end_time = row[headers['结束时间']]
            logging.debug(f'end_time: {end_time}')
        
            db_handler = DatabaseHandler(self.mysql_info)
            db_handler.connect_db()
            query = f"SELECT id FROM node_line WHERE node_line_name = '{node_name}'"
            
            is_single, data = db_handler.is_single_row(query)
            if is_single:
                logging.debug(f"{node_name} Query returned a single row: %s", data)
            elif data:
                logging.error("Query returned multiple rows: %s", data)
                raise Exception(f"{node_name} Query returned multiple rows")
            else:
                logging.error(f"{node_name} Query returned no rows")
                raise Exception(f"{node_name} Query returned no rows")

            db_handler.disconnect_db()
            
            data_file, file_num = self.get_random_file()
            
            logging.info(f'node_line_name: {node_name}; node_line_id: {data[0]}; bandwidth: {bandwidth}; start_time: {start_time}; end_time: {end_time}; file_num: {file_num}')
            
            if bandwidth == 0 and start_time == 0 and end_time == 0:
                logging.info(f"{node_name} No traffic flow data")
                continue
            

            self.thread_run(data_file, file_num, bandwidth, start_time, end_time, node_name, data[0])
            
            '''
            # node_line 有重复的无法使用多线程
            thread = threading.Thread(target=self.thread_run, 
                                      args=(
                                                data_file, 
                                                file_num, 
                                                bandwidth, 
                                                start_time, 
                                                end_time, 
                                                node_name, 
                                                data[0])
                                      )
            thread.start()
            '''


class PNodeLineInsertDatabase(object):
    
    def __init__(self, mysql_info):
        self.mysql_info = mysql_info
        self.db_handler = DatabaseHandler(self.mysql_info)
        self.directory = f'{os.path.dirname(home_dire)}/traffic/physical_node_line'
        self.db_handler.connect_db()
        
    def inter_database_traffic(self, sql):
        self.db_handler.insert_data(sql)
        
    def delete_database_traffic(self, sql):
        self.db_handler.delete_data(sql)    

    def get_files(self):
        
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
                    
                    
                    logging.debug(f'delete sql: {delete_sql}')
                    self.delete_database_traffic(delete_sql)
                    
                    logging.debug(f'insert sql: {query}')
                    self.inter_database_traffic(query)
                
                logging.info(f'Physical_node_line file: {file} insert success')
        
        self.db_handler.disconnect_db()

