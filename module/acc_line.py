#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import openpyxl
import datetime
import json
import os
import random
import re
from module.traffice import CETrafficFlow
from module.database_query import DatabaseHandler
from module.log import Log

logging = Log(__file__).getlog()

home_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class AccLine(object):
    def __init__(self, file_name):
        self.file_name = file_name
        # self.directory = os.path.join(home_dir, 'traffic', 'ce_device_interface')
    
    def save_data(self, data, path_dir, interface_name):
        # node_name_dict_keys = data.keys()
        node_name = interface_name
        
        traffic_files_path = path_dir
        all_files = os.listdir(traffic_files_path)
        files = [f for f in all_files if os.path.isfile(os.path.join(traffic_files_path, f))]
        
        if node_name in files:
            with open(f'{traffic_files_path}/{node_name}', 'r', encoding='utf-8') as f:
                data_old = json.load(f)
                
            data_old.update(data)

            with open(f'{traffic_files_path}/{node_name}', 'w', encoding='utf-8') as f:
                json.dump(data_old, f, ensure_ascii=False, indent=4)
                
            logging.info(f'{node_name} has been updated')
        else:
            with open(f'{traffic_files_path}/{node_name}', 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
                
            logging.info(f'{node_name} has been saved')
    
    def get_random_file(self):
        
        # 获取当前文件目录父目录
        directory = home_dir
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
    
    def thread_run(self,data_file, file_num, bandwidth, start_time, end_time):
        trafficeflow = CETrafficFlow(data_file, int(file_num), int(bandwidth), int(start_time), int(end_time))
        trafficeflow_all = trafficeflow.get_traffic_flow()
        trafficeflow_gen = trafficeflow.generate_traffic()
        return trafficeflow_gen
        # self.save_data(trafficeflow_gen)
        # logging.debug(trafficeflow_gen)
    
    def get_ce_device_interface_sheet(self):
        wb = openpyxl.load_workbook(self.file_name)
        
        sheet = wb['接入线路']
        
        headers = {cell.value: cell.column - 1 for cell in sheet[1]}
        
        for row in sheet.iter_rows(min_row=2, values_only=True):
        
            acc_node_code = row[headers['节点code']]
            acc_pe_device_name = row[headers['PE设备名']]
            acc_pe_device_code = row[headers['PE设备code']]
            acc_pe_main_interface_name = row[headers['PE物理接口']]
            acc_pe_main_interface_code = row[headers['PE物理接口code']]
            monitor_period = 5
            start_time = row[headers['开始时间']]
            end_time = row[headers['结束时间']]
            bandwidth = row[headers['带宽(M)']]


            logging.debug(f'bandwidth: {bandwidth}')
            logging.debug(f'start_time: {start_time}')
            logging.debug(f'end_time: {end_time}')
            logging.debug(f'acc_node_code: {acc_node_code}')
            logging.debug(f'acc_pe_device_name: {acc_pe_device_name}')
            logging.debug(f'acc_pe_device_code: {acc_pe_device_code}')
            logging.debug(f'acc_pe_main_interface_name: {acc_pe_main_interface_name}')
            logging.debug(f'acc_pe_main_interface_code: {acc_pe_main_interface_code}')
            logging.debug(f'monitor_period: {monitor_period}')
            logging.debug('===========================================================================')
            
            data_file, file_num = self.get_random_file()
            
            
            logging.info(f'service_interface:{acc_pe_device_name}_{acc_pe_main_interface_name}; bandwidth: {bandwidth}; start_time: {start_time}; end_time: {end_time}; file_num: {file_num}')
            traff_flow = self.thread_run(data_file, file_num, bandwidth, start_time, end_time)
            pe_traff_flow = traff_flow.copy()
            
            # traff_flow['customer_code'] = customer_code
            # traff_flow['site_code'] = site_code
            # traff_flow['ce_device_code'] = ce_device_code
            # traff_flow['ce_device_main_interface_code'] = ce_device_main_interface_code
            # traff_flow['ce_device_service_interface_code'] = ce_device_service_interface_code
            # traff_flow['monitor_period'] = monitor_period
            
            
            # ce_main_interface_dir = os.path.join(home_dir, 'traffic', 'acc_line')
            # ce_traffic_file_name = f'{ce_device_main_interface_name}_{ce_device_service_interface_name}'
            # self.save_data(traff_flow, ce_main_interface_dir, ce_traffic_file_name)
            
            if acc_node_code is not None and acc_pe_device_name is not None and acc_pe_device_code is not None and acc_pe_main_interface_name is not None and acc_pe_main_interface_code is not None:
                logging.debug(f'acc_node_name: {acc_pe_device_name}; acc_pe_main_interface_name: {acc_pe_main_interface_name}')
                pe_traff_flow['node_code'] = acc_node_code
                pe_traff_flow['pe_device_code'] = acc_pe_device_code
                pe_traff_flow['pe_device_main_interface_code'] = acc_pe_main_interface_code
                pe_traff_flow['monitor_period'] = monitor_period
                pe_traffic_file_name = f'{acc_pe_device_name}_{acc_pe_main_interface_name}'
                pe_traffic_file_dir = os.path.join(home_dir, 'traffic', 'acc_line')
                self.save_data(pe_traff_flow, pe_traffic_file_dir, pe_traffic_file_name)
                # 合并数据
                # merged_data = {
                #     'node_code': acc_node_code,
                #     'pe_device_code': acc_pe_device_code,
                #     'pe_device_main_interface_code': acc_pe_main_interface_code,
                #     'monitor_period': 5,
                # }
                
            else:
                logging.info(f'acc_node_name: {acc_pe_device_name}; acc_pe_main_interface_name: {acc_pe_main_interface_name}')


class AccLineInsertDatabase(object):
    def __init__(self, mysql_info):
        self.mysql_info = mysql_info
        self.db_handler = DatabaseHandler(self.mysql_info)
        self.directory = f'{home_dir}/traffic/acc_line'
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

                node_code = ce_interface_data_dict['node_code']
                pe_device_code = ce_interface_data_dict['pe_device_code']
                pe_device_main_interface_code = ce_interface_data_dict['pe_device_main_interface_code']
                monitor_period = ce_interface_data_dict['monitor_period']
                
                logging.debug(f'node_code: {node_code}')
                logging.debug(f'pe_device_code: {pe_device_code}')
                logging.debug(f'monitor_period: {monitor_period}')
                logging.debug(f'pe_device_main_interface_code: {pe_device_main_interface_code}')
                
                values_to_remove = ['node_code',
                                    'pe_device_code',
                                    'pe_device_main_interface_code',
                                    'monitor_period'
                ]
                
  
                filtered_numbers = [num for num in ce_interface_data_dict_keys if num not in values_to_remove]
                
                for key in filtered_numbers:
                    logging.debug(f'key: {key}')
                    tx_traffic = ce_interface_data_dict[key]['tx_traffic']
                    rx_traffic = ce_interface_data_dict[key]['rx_traffic']
                    
                    formatted_date = get_formatted_date_from_timestamp(int(key) / 1000)
                
                    query_ce_main = f'INSERT into pe_device_main_interface_traffic_{formatted_date}(node_code,pe_device_code,pe_device_main_interface_code,data_time,monitor_period,rx_traffic,tx_traffic) VALUES ("{node_code}","{pe_device_code}","{pe_device_main_interface_code}",{key},{monitor_period},"{rx_traffic}","{tx_traffic}");'
                    
                    delete_ce_main = f'DELETE FROM pe_device_main_interface_traffic_{formatted_date} WHERE node_code = "{node_code}" AND pe_device_code = "{pe_device_code}" AND pe_device_main_interface_code = "{pe_device_main_interface_code}" AND data_time = {key};'
                    
                    logging.debug(f'delete: {delete_ce_main}')
                    self.delete_database_traffic(delete_ce_main)
                    
                    logging.debug(f'insert: {query_ce_main}')
                    self.inter_database_traffic(query_ce_main)
                    
                logging.info(f'CE_interface file: {file} insert success')
        
        self.db_handler.disconnect_db()