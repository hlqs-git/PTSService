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


class CEDevice(object):
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
        
        sheet = wb['CE']
        
        headers = {cell.value: cell.column - 1 for cell in sheet[1]}
        
        for row in sheet.iter_rows(min_row=2, values_only=True):
            
            customer_code = row[headers['客户code']]
            site_code = row[headers['站点code']]
            ce_device_code = row[headers['设备code']]
            ce_device_main_interface_name = row[headers['物理接口']]
            ce_device_main_interface_code = row[headers['物理接口code']]
            ce_device_service_interface_name = row[headers['业务接口']]
            ce_device_service_interface_code = row[headers['业务接口code']]
            bandwidth = row[headers['带宽(M)']]
            
            if bandwidth is not None:
                
                start_time = row[headers['开始时间']]
                end_time = row[headers['结束时间']]
                acc_node_code = row[headers['接入节点code']]
                acc_pe_device_name = row[headers['接入PE设备名']]
                acc_pe_device_code = row[headers['接入PE设备code']]
                acc_pe_main_interface_name = row[headers['接入PE物理接口']]
                acc_pe_main_interface_code = row[headers['PE物理接口code']]
                monitor_period = 5

                logging.debug(f'customer_code: {customer_code}')
                logging.debug(f'site_code: {site_code}')
                logging.debug(f'ce_device_code: {ce_device_code}')
                logging.debug(f'ce_device_main_interface_name: {ce_device_main_interface_name}')
                logging.debug(f'ce_device_main_interface_code: {ce_device_main_interface_code}')
                logging.debug(f'ce_device_service_interface_name: {ce_device_service_interface_name}')
                logging.debug(f'ce_device_service_interface_code: {ce_device_service_interface_code}')
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
            else:
                logging.info(f'{ce_device_service_interface_name} The bandwidth is empty and is skipped')
                continue
            
            data_file, file_num = self.get_random_file()
            
            
            logging.info(f'service_interface:{ce_device_service_interface_name}; bandwidth: {bandwidth}; start_time: {start_time}; end_time: {end_time}; file_num: {file_num}')
            traff_flow = self.thread_run(data_file, file_num, bandwidth, start_time, end_time)
            pe_traff_flow = traff_flow.copy()
            
            traff_flow['customer_code'] = customer_code
            traff_flow['site_code'] = site_code
            traff_flow['ce_device_code'] = ce_device_code
            traff_flow['ce_device_main_interface_code'] = ce_device_main_interface_code
            traff_flow['ce_device_service_interface_code'] = ce_device_service_interface_code
            traff_flow['monitor_period'] = monitor_period
            
            
            ce_main_interface_dir = os.path.join(home_dir, 'traffic', 'ce_device_interface')
            ce_traffic_file_name = f'{ce_device_main_interface_name}_{ce_device_service_interface_code}'
            self.save_data(traff_flow, ce_main_interface_dir, ce_traffic_file_name)
            
            # if acc_node_code is not None and acc_pe_device_name is not None and acc_pe_device_code is not None and acc_pe_main_interface_name is not None and acc_pe_main_interface_code is not None:
            #     logging.debug(f'acc_node_name: {acc_pe_device_name}; acc_pe_main_interface_name: {acc_pe_main_interface_name}')
            #     pe_traff_flow['node_code'] = acc_node_code
            #     pe_traff_flow['pe_device_code'] = acc_pe_device_code
            #     pe_traff_flow['pe_device_main_interface_code'] = acc_pe_main_interface_code
            #     pe_traff_flow['monitor_period'] = monitor_period
            #     pe_traffic_file_name = f'{acc_pe_device_name}_{acc_pe_main_interface_name}'
            #     pe_traffic_file_dir = os.path.join(home_dir, 'traffic', 'pe_physical_interface')
            #     self.save_data(pe_traff_flow, pe_traffic_file_dir, pe_traffic_file_name)
            # else:
            #     logging.info(f'acc_node_name: {acc_pe_device_name}; acc_pe_main_interface_name: {acc_pe_main_interface_name}')


class CEInsertDatabase(object):
    def __init__(self, mysql_info):
        self.mysql_info = mysql_info
        self.db_handler = DatabaseHandler(self.mysql_info)
        self.directory = f'{home_dir}/traffic/ce_device_interface'
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
                ce_device_service_interface_code = ce_interface_data_dict['ce_device_service_interface_code']
                monitor_period = ce_interface_data_dict['monitor_period']
                
                logging.debug(f'customer_code: {customer_code}')
                logging.debug(f'site_code: {site_code}')
                logging.debug(f'ce_device_code: {ce_device_code}')
                logging.debug(f'monitor_period: {monitor_period}')
                logging.debug(f'ce_device_main_interface_code: {ce_device_main_interface_code}')
                logging.debug(f'ce_device_service_interface_code: {ce_device_service_interface_code}')
                
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
                    query_ce_service = f'INSERT into ce_device_service_interface_traffic_{formatted_date}(customer_code,site_code,ce_device_code,ce_device_main_interface_code,ce_device_service_interface_code,monitor_period,data_time,rx_traffic,tx_traffic) VALUES ("{customer_code}","{site_code}","{ce_device_code}","{ce_device_main_interface_code}","{ce_device_service_interface_code}",{monitor_period},{key},"{rx_traffic}","{tx_traffic}");'
                    
                    delete_ce_main = f'DELETE FROM ce_device_main_interface_traffic_{formatted_date} WHERE customer_code = "{customer_code}" AND site_code = "{site_code}" AND ce_device_code = "{ce_device_code}" AND ce_device_main_interface_code = "{ce_device_main_interface_code}" AND data_time = {key};'
                    delete_ce_service = f'DELETE FROM ce_device_service_interface_traffic_{formatted_date} WHERE customer_code = "{customer_code}" AND site_code = "{site_code}" AND ce_device_code = "{ce_device_code}" AND ce_device_main_interface_code = "{ce_device_main_interface_code}" AND ce_device_service_interface_code = "{ce_device_service_interface_code}" AND data_time = {key};'
                    
                    logging.debug(f'delete: {delete_ce_main}')
                    logging.debug(f'delete: {delete_ce_service}')
                    self.delete_database_traffic(delete_ce_main)
                    self.delete_database_traffic(delete_ce_service)
                    
                    logging.debug(f'insert: {query_ce_main}')
                    logging.debug(f'insert: {query_ce_service}')
                    self.inter_database_traffic(query_ce_main)
                    self.inter_database_traffic(query_ce_service)
                    
                logging.info(f'CE_interface file: {file} insert success')
        
        self.db_handler.disconnect_db()

'''
from module.Database import DatabaseHandler
class CEInsertDatabase(object):
    def __init__(self, mysql_info):
        self.mysql_info = mysql_info
        self.db_handler = DatabaseHandler(self.mysql_info)
        self.directory = f'{home_dir}/traffic/ce_device_interface'
        self.db_handler.connect_db()

    def inter_database_traffic(self, query, params=None):
        self.db_handler.insert_data(query, params)

    def batch_insert(self, query, params_list):
        self.db_handler.insert_data_many(query, params_list)

    def start_transaction(self):
        self.db_handler.start_transaction()

    def commit_transaction(self):
        self.db_handler.commit()

    def rollback_transaction(self):
        self.db_handler.rollback()

    def get_file(self):
        def get_formatted_date_from_timestamp(timestamp):
            # 将时间戳转换为datetime对象
            dt_object = datetime.datetime.fromtimestamp(timestamp)
            # 格式化日期为YYYYMM
            formatted_date = dt_object.strftime('%Y%m')
            return formatted_date

        all_files = os.listdir(self.directory)
        files = [f for f in all_files if os.path.isfile(os.path.join(self.directory, f))]

        try:
            self.start_transaction()  # 开启事务

            for file in files:
                logging.debug(f'open file: {file}')
                with open(f'{self.directory}/{file}', 'r') as f:
                    ce_interface_data_dict = json.load(f)

                    customer_code = ce_interface_data_dict['customer_code']
                    site_code = ce_interface_data_dict['site_code']
                    ce_device_code = ce_interface_data_dict['ce_device_code']
                    ce_device_main_interface_code = ce_interface_data_dict['ce_device_main_interface_code']
                    ce_device_service_interface_code = ce_interface_data_dict['ce_device_service_interface_code']
                    monitor_period = ce_interface_data_dict['monitor_period']

                    logging.debug(f'customer_code: {customer_code}')
                    logging.debug(f'site_code: {site_code}')
                    logging.debug(f'ce_device_code: {ce_device_code}')
                    logging.debug(f'monitor_period: {monitor_period}')
                    logging.debug(f'ce_device_main_interface_code: {ce_device_main_interface_code}')
                    logging.debug(f'ce_device_service_interface_code: {ce_device_service_interface_code}')

                    values_to_remove = ['customer_code', 'site_code', 'ce_device_code', 'ce_device_main_interface_code',
                                       'ce_device_service_interface_code', 'monitor_period']

                    filtered_numbers = [num for num in ce_interface_data_dict.keys() if num not in values_to_remove]

                    params_list_ce_main = []
                    params_list_ce_service = []

                    for key in filtered_numbers:
                        tx_traffic = ce_interface_data_dict[key]['tx_traffic']
                        rx_traffic = ce_interface_data_dict[key]['rx_traffic']

                        formatted_date = get_formatted_date_from_timestamp(int(key) / 1000)

                        params_ce_main = (
                            formatted_date, customer_code, site_code, ce_device_code, ce_device_main_interface_code,
                            key, monitor_period, rx_traffic, tx_traffic)
                        params_ce_service = (
                            formatted_date, customer_code, site_code, ce_device_code, ce_device_main_interface_code,
                            ce_device_service_interface_code, monitor_period, key, rx_traffic, tx_traffic)

                        params_list_ce_main.append(params_ce_main)
                        params_list_ce_service.append(params_ce_service)

                        
                    query_ce_main = 'INSERT INTO ce_device_main_interface_traffic_%s (customer_code, site_code, ce_device_code, ce_device_main_interface_code, data_time, monitor_period, rx_traffic, tx_traffic) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);'
                    query_ce_service = 'INSERT INTO ce_device_service_interface_traffic_%s (customer_code, site_code, ce_device_code, ce_device_main_interface_code, ce_device_service_interface_code, monitor_period, data_time, rx_traffic, tx_traffic) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);'

                    # 批量插入数据
                    self.batch_insert(query_ce_main, params_list_ce_main)
                    self.batch_insert(query_ce_service, params_list_ce_service)

                    logging.info(f'CE_interface file: {file} insert success')

            self.commit_transaction()  # 提交事务

        except Exception as e:
            logging.error(f'Error inserting data: {e}')
            self.rollback_transaction()  # 发生异常时回滚事务

        finally:
            self.db_handler.disconnect_db()  # 最终断开数据库连接
'''

'''
# import os
# import json
# import datetime
# import logging
from module.Database import DatabaseHandler  # 假设你的 DatabaseHandler 类在 module 文件夹中

# 配置日志
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class CEInsertDatabase:
    def __init__(self, mysql_info):
        self.mysql_info = mysql_info
        self.db_handler = DatabaseHandler(self.mysql_info)
        self.home_dir = home_dir
        self.directory = f'{self.home_dir}/traffic/ce_device_interface'
        self.db_handler.connect_db()

    def inter_database_traffic(self, query, params=None):
        self.db_handler.insert_data(query, params)

    def batch_insert(self, query, params_list):
        self.db_handler.insert_data_many(query, params_list)

    def start_transaction(self):
        self.db_handler.start_transaction()

    def commit_transaction(self):
        self.db_handler.commit()

    def rollback_transaction(self):
        self.db_handler.rollback()

    def get_file(self):
        def get_formatted_date_from_timestamp(timestamp):
            dt_object = datetime.datetime.fromtimestamp(timestamp)
            formatted_date = dt_object.strftime('%Y%m')
            return formatted_date

        all_files = os.listdir(self.directory)
        files = [f for f in all_files if os.path.isfile(os.path.join(self.directory, f))]

        try:
            self.start_transaction()  # 开启事务

            for file in files:
                logging.debug(f'open file: {file}')
                with open(f'{self.directory}/{file}', 'r') as f:
                    logging.info(f'open file: {file} success')
                    ce_interface_data_dict = json.load(f)

                    customer_code = ce_interface_data_dict['customer_code']
                    site_code = ce_interface_data_dict['site_code']
                    ce_device_code = ce_interface_data_dict['ce_device_code']
                    ce_device_main_interface_code = ce_interface_data_dict['ce_device_main_interface_code']
                    ce_device_service_interface_code = ce_interface_data_dict['ce_device_service_interface_code']
                    monitor_period = ce_interface_data_dict['monitor_period']

                    logging.debug(f'customer_code: {customer_code}')
                    logging.debug(f'site_code: {site_code}')
                    logging.debug(f'ce_device_code: {ce_device_code}')
                    logging.debug(f'monitor_period: {monitor_period}')
                    logging.debug(f'ce_device_main_interface_code: {ce_device_main_interface_code}')
                    logging.debug(f'ce_device_service_interface_code: {ce_device_service_interface_code}')

                    values_to_remove = ['customer_code', 'site_code', 'ce_device_code', 'ce_device_main_interface_code',
                                       'ce_device_service_interface_code', 'monitor_period']

                    filtered_numbers = [num for num in ce_interface_data_dict.keys() if num not in values_to_remove]

                    params_list_ce_main = []
                    params_list_ce_service = []

                    for key in filtered_numbers:
                        logging.info(f'key: {key}')
                        tx_traffic = ce_interface_data_dict[key]['tx_traffic']
                        rx_traffic = ce_interface_data_dict[key]['rx_traffic']

                        formatted_date = get_formatted_date_from_timestamp(int(key) / 1000)

                        query_ce_main = f"INSERT INTO ce_device_main_interface_traffic_{formatted_date} (customer_code, site_code, ce_device_code, ce_device_main_interface_code, data_time, monitor_period, rx_traffic, tx_traffic) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
                        query_ce_service = f"INSERT INTO ce_device_service_interface_traffic_{formatted_date} (customer_code, site_code, ce_device_code, ce_device_main_interface_code, ce_device_service_interface_code, monitor_period, data_time, rx_traffic, tx_traffic) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"

                        params_ce_main = (
                            customer_code, site_code, ce_device_code, ce_device_main_interface_code,
                            key, monitor_period, rx_traffic, tx_traffic)
                        params_ce_service = (
                            customer_code, site_code, ce_device_code, ce_device_main_interface_code,
                            ce_device_service_interface_code, monitor_period, key, rx_traffic, tx_traffic)

                        params_list_ce_main.append((query_ce_main, params_ce_main))
                        params_list_ce_service.append((query_ce_service, params_ce_service))

                    # 批量插入数据
                    for query, params_list in params_list_ce_main:
                        self.batch_insert(query, [params_list])
                    for query, params_list in params_list_ce_service:
                        self.batch_insert(query, [params_list])

                    logging.info(f'CE_interface file: {file} insert success')

            self.commit_transaction()  # 提交事务

        except Exception as e:
            logging.error(f'Error inserting data: {e}')
            self.rollback_transaction()  # 发生异常时回滚事务

        finally:
            self.db_handler.disconnect_db()  # 最终断开数据库连接

# 示例使用
if __name__ == "__main__":
    mysql_info = {
        'host': '192.168.12.51',
        'user': 'root',
        'password': 'Root@123!@#',
        'database': 'pts_oam_center_demo_2.1.0',
        'port': 3306
    }
    
    home_dir = '/path/to/home/directory'  # 你的主目录路径
    
    ce_insert_db = CEInsertDatabase(mysql_info, home_dir)
    ce_insert_db.get_file()
'''
        

