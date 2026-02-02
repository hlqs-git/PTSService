#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from module.log import Log
from module.database_query import DatabaseHandler
import datetime
import openpyxl
import os
import json


logging = Log(__file__).getlog()

home_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class PEPhysicsInterface(object):
    def __init__(self, filename):
        self.filename = filename
        self.directory = f'{home_dir}/traffic/pe_physical_interface'
        self.physics_node_line_directory = f'{home_dir}/traffic/physical_node_line'
        
    
    def get_pe_physics_interface_sheet(self):
        wb = openpyxl.load_workbook(self.filename)
        sheet = wb['PE']
        
        headers = {cell.value: cell.column - 1 for cell in sheet[1]}
        
        # 定义一个函数，用于合并两个用 '|' 分割的字符串，并返回合并后的结果
        def merge_traffic(existing, new):
            existing_values = list(map(int, existing.split('|')))
            new_values = list(map(int, new.split('|')))
            merged_values = [x + y for x, y in zip(existing_values, new_values)]
            return '|'.join(map(str, merged_values))
        
        # 定义一个函数，用于遍历文件列表，读取每个文件并将其数据合并到 merged_data 字典中
        def merge_dicts(file_list, merged_data):
            for file_name in file_list:
                try:
                    with open(f'{self.physics_node_line_directory}/{file_name}', 
                            'r', encoding='utf-8') as file:
                        data = json.load(file)
                        for key, value in data.items():
                            for timestamp, metrics in value.items():
                                if timestamp not in merged_data:
                                    merged_data[timestamp] = {
                                        "tx_traffic": metrics["tx_traffic"],
                                        "rx_traffic": metrics["rx_traffic"]
                                    }
                                else:
                                    merged_data[timestamp]["tx_traffic"] = merge_traffic(
                                        merged_data[timestamp]["tx_traffic"], metrics["tx_traffic"])
                                    merged_data[timestamp]["rx_traffic"] = merge_traffic(
                                        merged_data[timestamp]["rx_traffic"], metrics["rx_traffic"])
                except Exception as e:
                    logging.error(f'Error reading file {file_name}: {e}')
                    raise f'Error reading file {file_name}: {e}'
                    
        
        for row in sheet.iter_rows(min_row=2, values_only=True):
            
            node_name = row[headers['节点名称']]
            pe_device_name = row[headers['PE设备名']]
            pe_physics_interface_name = row[headers['PE物理接口']]
            
            logging.debug(f'name: {node_name} - {pe_device_name} - {pe_physics_interface_name}')
            
            node_code = row[headers['节点code']]
            pe_device_code = row[headers['PE设备code']]
            pe_physics_interface_code = row[headers['PE物理接口code']]
            physics_node_line = row[headers['对应节点线路']]
            
            logging.debug(f'code: {node_code} - {pe_device_code} - {pe_physics_interface_code}')

            if physics_node_line is not None:
                physics_node_line_list = physics_node_line.split(',')
                
                # 合并数据
                merged_data = {
                    'node_code': node_code,
                    'pe_device_code': pe_device_code,
                    'pe_device_main_interface_code': pe_physics_interface_code,
                    'monitor_period': 5,
                }
                merge_dicts(physics_node_line_list, merged_data)
                
                logging.info(f'node_info: {node_name} - {pe_device_name} - {pe_physics_interface_name}')
                
                with open(f'{self.directory}/{pe_device_name}_{pe_physics_interface_name}', 
                          'w', encoding='utf-8') as file:
                    json.dump(merged_data, file, ensure_ascii=False, indent=4)
                
                logging.info(f'{pe_device_name}_{pe_physics_interface_name} 写入成功')
            else:
                continue


class PEMainInterfaceInsertDatabase():
    def __init__(self, mysql_info):
        self.mysql_info = mysql_info
        self.db_handler = DatabaseHandler(self.mysql_info)
        self.db_handler.connect_db()
        self.directory = f'{home_dir}/traffic/pe_physical_interface'
    
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
                pe_main_interface_data_dict = json.load(f)
                pe_main_interface_data_dict_keys = list(pe_main_interface_data_dict.keys())

                node_code = pe_main_interface_data_dict['node_code']
                pe_device_code = pe_main_interface_data_dict['pe_device_code']
                pe_device_main_interface_code = pe_main_interface_data_dict['pe_device_main_interface_code']
                monitor_period = pe_main_interface_data_dict['monitor_period']
                logging.debug(f'node_code: {node_code}')
                logging.debug(f'pe_device_code: {pe_device_code}')
                logging.debug(f'pe_device_main_interface_code: {pe_device_main_interface_code}')
                logging.debug(f'monitor_period: {monitor_period}')
                
                values_to_remove = ['node_code', 
                                    'pe_device_code', 
                                    'pe_device_main_interface_code', 
                                    'monitor_period']
                
                filtered_numbers = [num for num in pe_main_interface_data_dict_keys if num not in values_to_remove]
                
                for key in filtered_numbers:
                    tx_traffic = pe_main_interface_data_dict[key]['tx_traffic']
                    rx_traffic = pe_main_interface_data_dict[key]['rx_traffic']
                    
                    formatted_date = get_formatted_date_from_timestamp(int(key) / 1000)
                
                    query = f'INSERT into pe_device_main_interface_traffic_{formatted_date}(node_code,pe_device_code,pe_device_main_interface_code,monitor_period,data_time,tx_traffic,rx_traffic) VALUES ("{node_code}","{pe_device_code}","{pe_device_main_interface_code}",{monitor_period},{key},"{tx_traffic}","{rx_traffic}");'
                    delete_sql = f'DELETE FROM pe_device_main_interface_traffic_{formatted_date} WHERE node_code = "{node_code}" AND pe_device_code = "{pe_device_code}" AND pe_device_main_interface_code = "{pe_device_main_interface_code}" AND data_time = {key};'
                    
                    logging.debug(f'delete: {delete_sql}')
                    self.delete_database_traffic(delete_sql)
                    
                    logging.debug(f'insert: {query}')
                    self.inter_database_traffic(query)
                    
                logging.info(f'PE_main_interface file: {file} insert success')
        
        self.db_handler.disconnect_db()



        
        