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


class CEMainInterface(object):
    def __init__(self, filename):
        self.filename = filename
        self.directory = f'{home_dir}/traffic/ce_main_interface'
        self.ce_service_interface = f'{home_dir}/traffic/ce_device_interface'
        
    
    def get_ce_physics_interface_sheet(self):
        wb = openpyxl.load_workbook(self.filename)
        sheet = wb['物理接口补充表']
        
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
                    with open(f'{self.ce_service_interface}/enp2s0_{file_name}', 
                            'r', encoding='utf-8') as file:
                        data = json.load(file)
                        
                        values_to_remove = ['customer_code',
                                            'site_code',
                                            'ce_device_code',
                                            'ce_device_main_interface_code',
                                            'ce_device_service_interface_code',
                                            'monitor_period'
                        ]
                
  
                        filtered_numbers = [num for num in data if num not in values_to_remove]
                        

                        for key in filtered_numbers:
                            if key not in merged_data:
                                merged_data[key] = {
                                    "tx_traffic": data[key]["tx_traffic"],
                                    "rx_traffic": data[key]["rx_traffic"]
                                }
                            else:
                                merged_data[key]["tx_traffic"] = merge_traffic(
                                    merged_data[key]["tx_traffic"], data[key]["tx_traffic"])
                                merged_data[key]["rx_traffic"] = merge_traffic(
                                    merged_data[key]["rx_traffic"], data[key]["rx_traffic"])
                except Exception as e:
                    logging.error(f'Error reading file {file_name}: {e}')
                    raise f'Error reading file {file_name}: {e}'
                    
        
        for row in sheet.iter_rows(min_row=2, values_only=True):
            
            node_name = row[headers['客户']]
            site_name = row[headers['站点名称']]
            
            logging.debug(f'name: {node_name} - {site_name}')
            
            node_code = row[headers['客户code']]
            site_code = row[headers['站点code']]
            device_code = row[headers['设备code']]
            physics_interface_code = row[headers['物理接口code']]
            service_interface = row[headers['业务接口']]
            
            # logging.debug(f'code: {node_code} - {pe_device_code} - {pe_physics_interface_code}')

            if service_interface is not None:
                physics_node_line_list = service_interface.split(',')
                
                # 合并数据
                merged_data = {
                    'customer_code': node_code,
                    'site_code': site_code,
                    'ce_device_code': device_code,
                    'ce_device_main_interface_code': physics_interface_code,
                    'monitor_period': 5,
                }
                merge_dicts(physics_node_line_list, merged_data)
                
                logging.info(f'CE_site: {node_name} - {site_name}')
                
                with open(f'{self.directory}/{node_code}_{site_code}', 
                          'w', encoding='utf-8') as file:
                    json.dump(merged_data, file, ensure_ascii=False, indent=4)
                
                logging.info(f'{node_code}_{site_code} 写入成功')
            else:
                continue


class CEMainInsertDatabase(object):
    def __init__(self, mysql_info):
        self.mysql_info = mysql_info
        self.db_handler = DatabaseHandler(self.mysql_info)
        self.directory = f'{home_dir}/traffic/ce_main_interface'
        self.db_handler.connect_db()
        
        
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
            logging.info(f'{file} is being processed.')

            with open(f'{self.directory}/{file}', 'r') as f:
                logging.debug(f'open file: {file}')
                
                ce_interface_data_dict = json.load(f)
                ce_interface_data_dict_keys = list(ce_interface_data_dict.keys())

                customer_code = ce_interface_data_dict['customer_code']
                site_code = ce_interface_data_dict['site_code']
                ce_device_code = ce_interface_data_dict['ce_device_code']
                ce_device_main_interface_code = ce_interface_data_dict['ce_device_main_interface_code']
                # ce_device_service_interface_code = ce_interface_data_dict['ce_device_service_interface_code']
                monitor_period = ce_interface_data_dict['monitor_period']
                
                logging.debug(f'customer_code: {customer_code}')
                logging.debug(f'site_code: {site_code}')
                logging.debug(f'ce_device_code: {ce_device_code}')
                logging.debug(f'monitor_period: {monitor_period}')
                logging.debug(f'ce_device_main_interface_code: {ce_device_main_interface_code}')
                # logging.debug(f'ce_device_service_interface_code: {ce_device_service_interface_code}')
                
                values_to_remove = ['customer_code',
                                    'site_code',
                                    'ce_device_code',
                                    'ce_device_main_interface_code',
                                    'ce_device_service_interface_code',
                                    'monitor_period'
                ]
                
  
                filtered_numbers = [num for num in ce_interface_data_dict_keys if num not in values_to_remove]
                
                for key in filtered_numbers:
                    logging.debug(f'key: {key}')
                    tx_traffic = ce_interface_data_dict[key]['tx_traffic']
                    rx_traffic = ce_interface_data_dict[key]['rx_traffic']
                    
                    formatted_date = get_formatted_date_from_timestamp(int(key) / 1000)
                
                    query_ce_main = f'INSERT into ce_device_main_interface_traffic_{formatted_date}(customer_code,site_code,ce_device_code,ce_device_main_interface_code,data_time,monitor_period,rx_traffic,tx_traffic) VALUES ("{customer_code}","{site_code}","{ce_device_code}","{ce_device_main_interface_code}",{key},{monitor_period},"{rx_traffic}","{tx_traffic}");'
                    # query_ce_service = f'INSERT into ce_device_service_interface_traffic_{formatted_date}(customer_code,site_code,ce_device_code,ce_device_main_interface_code,ce_device_service_interface_code,monitor_period,data_time,rx_traffic,tx_traffic) VALUES ("{customer_code}","{site_code}","{ce_device_code}","{ce_device_main_interface_code}","{ce_device_service_interface_code}",{monitor_period},{key},"{rx_traffic}","{tx_traffic}");'
                    
                    delete_ce_main = f'DELETE FROM ce_device_main_interface_traffic_{formatted_date} WHERE customer_code = "{customer_code}" AND site_code = "{site_code}" AND ce_device_code = "{ce_device_code}" AND ce_device_main_interface_code = "{ce_device_main_interface_code}" AND data_time = {key};'
                    # delete_ce_service = f'DELETE FROM ce_device_service_interface_traffic_{formatted_date} WHERE customer_code = "{customer_code}" AND site_code = "{site_code}" AND ce_device_code = "{ce_device_code}" AND ce_device_main_interface_code = "{ce_device_main_interface_code}" AND ce_device_service_interface_code = "{ce_device_service_interface_code}" AND data_time = {key};'
                    
                    logging.debug(f'delete: {delete_ce_main}')
                    # logging.debug(f'delete: {delete_ce_service}')
                    self.delete_database_traffic(delete_ce_main)
                    # self.delete_database_traffic(delete_ce_service)
                    
                    logging.debug(f'insert: {query_ce_main}')
                    # logging.debug(f'insert: {query_ce_service}')
                    self.inter_database_traffic(query_ce_main)
                    # self.inter_database_traffic(query_ce_service)
                    
                logging.info(f'CE_interface file: {file} insert success')
        
        self.db_handler.disconnect_db()