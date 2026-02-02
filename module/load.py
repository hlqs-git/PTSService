#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random
import openpyxl
import pandas as pd
import json
import datetime
from module.database_query import DatabaseHandler
from module.log import Log

logging = Log(__file__).getlog()

def get_formatted_date_from_timestamp(timestamp):
    # 将时间戳转换为datetime对象
    dt_object = datetime.datetime.fromtimestamp(timestamp)
    
    # 格式化日期为YYYYMM
    formatted_date = dt_object.strftime('%Y%m')
    
    return formatted_date

def random_num(numa, numb):
    """ 生成随机随机数 """
    num1 = round(random.uniform(numa, numb), 3)
    num2 = round(random.uniform(numa, numb), 3)
    num3 = round(random.uniform(numa, numb), 3)
    num4 = float(num1) + float(num2) + float(num3)
    num5 = float(round(num4, 3)) / 3
    return round(num5, 3)

def day_device_load_flow():
    mem_load_day = None
    cpu_load_day = None
    system_load_day_one = None
    system_load_day_five = None
    system_load_day_fifteen = None
    
    for counter in range(288):
        mem_load = random_num(0.10,0.40)
        cpu_load = random_num(0.05,0.10)
        system_load_one = random_num(0.1,0.5)
        system_load_five = random_num(0.1,0.4)
        system_load_fifteen = random_num(0.1,0.3)
        
        if counter == 0:
            mem_load_day = mem_load
            cpu_load_day = cpu_load
            system_load_day_one = system_load_one
            system_load_day_five = system_load_five
            system_load_day_fifteen = system_load_fifteen
        else:
            mem_load_day = f"{mem_load_day}|{mem_load}"
            cpu_load_day = f"{cpu_load_day}|{cpu_load}"
            system_load_day_one = f"{system_load_day_one}|{system_load_one}"
            system_load_day_five = f"{system_load_day_five}|{system_load_five}"
            system_load_day_fifteen = f"{system_load_day_fifteen}|{system_load_fifteen}"
            
    return {
        'cpu_usage_rate': cpu_load_day,
        'memory_usage_rate': mem_load_day,
        'one_min_system_load_average': system_load_day_one,
        'five_min_system_load_average': system_load_day_five,
        'fifteen_min_system_load_average': system_load_day_fifteen
    }
    
def all_device_load_flow(device_code, start_time=1609430400, end_time=1861804800):
    all_load = {
        device_code:{}
    }
    # start_time = 1609430400
    # end_time = 1861804800
    
    while True:
        all_load[device_code][start_time] = day_device_load_flow()
        
        start_time += 86400
        if start_time >= end_time:
            return all_load
        
    
def open_pe_workbook(file_path, mysql_info):
    
    db_handle = DatabaseHandler(mysql_info)
    db_handle.connect_db()

    df = pd.read_excel(file_path, sheet_name='PE', engine='openpyxl')

    # Check if the column exists
    if 'PE设备code' not in df.columns:
        
        raise ValueError(f"Column with title 'PE设备code' not found in sheet 'PE'")

    # Get the specified column and remove duplicates
    column_data = df['PE设备code'].drop_duplicates().tolist()

    for value in column_data:
        logging.info(f'PE {value} task start')

        all_load_data = all_device_load_flow(value)

        for key1 in list(all_load_data.keys()):

            for key2 in list(all_load_data[key1].keys()):
                time_ym = get_formatted_date_from_timestamp(key2)

                cpu = all_load_data[key1][key2]['cpu_usage_rate']
                mem = all_load_data[key1][key2]['memory_usage_rate']
                one_min = all_load_data[key1][key2]['one_min_system_load_average']
                five_min = all_load_data[key1][key2]['five_min_system_load_average']
                fifteen_min = all_load_data[key1][key2]['fifteen_min_system_load_average']
                
                delete_sql = f'DELETE FROM pe_device_load_{time_ym} WHERE pe_device_code = "{key1}" AND data_time = {int(key2) * 1000};'
                insert_sql = f'INSERT into pe_device_load_{time_ym} (pe_device_code,monitor_period,data_time,cpu_usage_rate,memory_usage_rate,one_min_system_load_average,five_min_system_load_average,fifteen_min_system_load_average) VALUES ("{key1}","5min",{int(key2) * 1000},"{cpu}","{mem}","{one_min}","{five_min}","{fifteen_min}");'

                db_handle.delete_data(delete_sql)
                db_handle.insert_data(insert_sql)
        
        logging.info(f'PE {value} task end')
    db_handle.disconnect_db()
"""
def open_ce_workbook(file_path, mysql_info):
    
    db_handle = DatabaseHandler(mysql_info)
    db_handle.connect_db()

    df = pd.read_excel(file_path, sheet_name='CE', engine='openpyxl')

    # Check if the column exists
    if '设备code' not in df.columns:
        
        raise ValueError(f"Column with title '设备code' not found in sheet 'PE'")

    # Get the specified column and remove duplicates
    column_data = df['设备code'].drop_duplicates().tolist()

    for value in column_data:
        logging.info(f'CE {value} task start')

        all_load_data = all_device_load_flow(value)

        for key1 in list(all_load_data.keys()):

            for key2 in list(all_load_data[key1].keys()):
                time_ym = get_formatted_date_from_timestamp(key2)

                cpu = all_load_data[key1][key2]['cpu_usage_rate']
                mem = all_load_data[key1][key2]['memory_usage_rate']
                one_min = all_load_data[key1][key2]['one_min_system_load_average']
                five_min = all_load_data[key1][key2]['five_min_system_load_average']
                fifteen_min = all_load_data[key1][key2]['fifteen_min_system_load_average']
                
                delete_sql = f'DELETE FROM ce_device_load_{time_ym} WHERE ce_device_code = "{key1}" AND data_time = {int(key2) * 1000};'
                insert_sql = f'INSERT into ce_device_load_{time_ym} (ce_device_code,monitor_period,data_time,cpu_usage_rate,memory_usage_rate,one_min_system_load_average,five_min_system_load_average,fifteen_min_system_load_average) VALUES ("{key1}",5,{int(key2) * 1000},"{cpu}","{mem}","{one_min}","{five_min}","{fifteen_min}");'

                db_handle.delete_data(delete_sql)
                db_handle.insert_data(insert_sql)
        
        logging.info(f'CE {value} task end')
    db_handle.disconnect_db()
"""

def open_ce_workbook(file_path, mysql_info):
    
    db_handle = DatabaseHandler(mysql_info)
    db_handle.connect_db()
    
    wb = openpyxl.load_workbook(file_path)
    
    sheet = wb['CE']
    
    headers = {cell.value: cell.column - 1 for cell in sheet[1]}
    
    for row in sheet.iter_rows(min_row=2, values_only=True):
        ce_device_code = row[headers['设备code']]
        start_time = row[headers['开始时间']]
        end_time = row[headers['结束时间']]
        
        if start_time is not None or end_time is not None:
        
            logging.info(f'CE {ce_device_code} task start; StartTime {start_time} EndTime {end_time}')
            
            all_load_data = all_device_load_flow(ce_device_code, start_time, end_time)
            
            for key1 in list(all_load_data.keys()):
                for key2 in list(all_load_data[key1].keys()):
                    time_ym = get_formatted_date_from_timestamp(key2)

                    cpu = all_load_data[key1][key2]['cpu_usage_rate']
                    mem = all_load_data[key1][key2]['memory_usage_rate']
                    one_min = all_load_data[key1][key2]['one_min_system_load_average']
                    five_min = all_load_data[key1][key2]['five_min_system_load_average']
                    fifteen_min = all_load_data[key1][key2]['fifteen_min_system_load_average']
                    
                    delete_sql = f'DELETE FROM ce_device_load_{time_ym} WHERE ce_device_code = "{key1}" AND data_time = {int(key2) * 1000};'
                    insert_sql = f'INSERT into ce_device_load_{time_ym} (ce_device_code,monitor_period,data_time,cpu_usage_rate,memory_usage_rate,one_min_system_load_average,five_min_system_load_average,fifteen_min_system_load_average) VALUES ("{key1}","5min",{int(key2) * 1000},"{cpu}","{mem}","{one_min}","{five_min}","{fifteen_min}");'

                    db_handle.delete_data(delete_sql)
                    db_handle.insert_data(insert_sql)
            
            logging.info(f'CE {ce_device_code} task end')
        else:
            logging.info(f'CE {ce_device_code} is not in the time range')
            continue
    db_handle.disconnect_db()
        

def pts_device_load(file_path, mysql_info):
    open_pe_workbook(file_path,mysql_info)
    open_ce_workbook(file_path,mysql_info)
    

# if __name__ == '__main__':
#     open_workbook('/home/liqing/project/python/agent/agent_debug_new/PTSTraffic/file/PTS_test.xlsx')
