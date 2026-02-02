#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from module.check_xlsx import check_data_types
from module.log import Log
from module.Physical_node_line import PhysicalNodeLine
from module.Physical_node_line import PNodeLineInsertDatabase
from module.Logical_node_line import LogicalNodeLine
from module.Logical_node_line import LNodeLineInsertDatabase
from module.pe_physics_interface import PEPhysicsInterface
from module.pe_physics_interface import PEMainInterfaceInsertDatabase
from module.ce_device import CEDevice
from module.ce_device import CEInsertDatabase
from module.acc_line import AccLine
from module.acc_line import AccLineInsertDatabase
from module.load import pts_device_load
from module.ce_main_interface_reuse import CEMainInsertDatabase
from module.ce_main_interface_reuse import CEMainInterface
import threading
import os

logging  = Log(__name__).getlog()
project_path = os.path.dirname(os.path.abspath(__file__))


# 定义数据文件
file_path = f'{project_path}/file/PTS_edit.xlsx'



expected_types = {
    '物理节点线路':{
        '节点线路名称':str,
        '带宽(M)':int,
        '开始时间':int,
        '结束时间':int
    },
    
    '逻辑节点线路':{
        '逻辑节点选路名称':str,
        '对应物理线路':str
    },
    
    'PE':{
        '节点名称':str,
        '节点code':str,
        'PE设备名':str,
        'pe设备code':str,
        'PE物理接口':str,
        'PE物理接口code':str,
        'PE物理接口类型':str,
        '对应节点线路':str  
    },
    
    'CE':{
        '客户名称':str,
        '客户code':str,
        '站点名称':str,
        '站点code':str,
        '站点类型':str,
        '设备code':str,
        '物理接口':str,
        '物理接口code':str,
        '业务接口':str,
        '业务接口code':str,
        '带宽(M)':int,
        '开始时间':int,
        '结束时间':int
    }
}

'''
# 校验数据
check_out = check_data_types(file_path, expected_types)

if check_out == 0 :
    logging.info("Check data types Success")
else:
    logging.error("Check data types Failed")
'''

# 定义数据库信息
mysql_info = {
    'host':'192.168.10.99', 
    'user':'LiQing', 
    'password':'Liqing0727!', 
    'database':'pts_oam_center_demo_2.1.0', 
    'port':3306
}

#mysql_info = {
#     'host':'192.168.12.51', 
#     'user':'root', 
#     'password':'Root@123!@#', 
#     'database':'pts_oam_center_demo_2.1.0', 
#     'port':3306
# }



def node_line_and_pe_device():
    # 生成数据链路数据 physical_node_line
    # 数据文件保存至 ../PTSTraffic/traffic/physical_node_line/
    physical_node_line_traffic_data = PhysicalNodeLine(file_path, mysql_info)
    physical_node_line_traffic_data.get_physical_node_line_sheet()

    # 生成逻辑链路数据 
    # 数据文件保存至 ../PTSTraffic/traffic/logical_node_line/
    logical_node_line_traffic_data = LogicalNodeLine(file_path, mysql_info)
    logical_node_line_traffic_data.get_logical_node_line_sheet()
    
    # physical_node_line 数据插入数据库
    insert_node_line_traffic = PNodeLineInsertDatabase(mysql_info)
    insert_node_line_traffic.get_files()

    # logical_node_line 数据插入数据库
    insert_logical_node_line_traffic = LNodeLineInsertDatabase(mysql_info)
    insert_logical_node_line_traffic.get_file()
    
    # 生成PE数据
    # 数据文件保存至 ../PTSTraffic/traffic/pe_physical_interface/
    pe_main_interface_traffic_data = PEPhysicsInterface(file_path)
    pe_main_interface_traffic_data.get_pe_physics_interface_sheet()

    # pe 数据插入数据库
    insert_pe_physics_interface_traffic = PEMainInterfaceInsertDatabase(mysql_info)
    insert_pe_physics_interface_traffic.get_file()


def ce_device():
    # 生成ce数据
    ce_interface_traffic_data = CEDevice(file_path)
    ce_interface_traffic_data.get_ce_device_interface_sheet()

    # ce 数据插入数据库
    insert_ce_device_interface_traffic = CEInsertDatabase(mysql_info)
    insert_ce_device_interface_traffic.get_file()

def acc_line():
    # 生成acc数据
    acc_line_traffic_data = AccLine(file_path)
    acc_line_traffic_data.get_ce_device_interface_sheet()
    
    # acc 数据插入数据库
    insert_acc_line_traffic = AccLineInsertDatabase(mysql_info)
    insert_acc_line_traffic.get_file()

def device_load():
    # 插入负载数据
    pts_device_load(file_path, mysql_info)

def pts_create_data():
    # 生成数据链路数据 physical_node_line
    # 数据文件保存至 ../PTSTraffic/traffic/physical_node_line/
    physical_node_line_traffic_data = PhysicalNodeLine(file_path, mysql_info)
    physical_node_line_traffic_data.get_physical_node_line_sheet()

    # 生成逻辑链路数据 
    # 数据文件保存至 ../PTSTraffic/traffic/logical_node_line/
    logical_node_line_traffic_data = LogicalNodeLine(file_path, mysql_info)
    logical_node_line_traffic_data.get_logical_node_line_sheet()
    
    # 生成PE数据
    # 数据文件保存至 ../PTSTraffic/traffic/pe_physical_interface/
    pe_main_interface_traffic_data = PEPhysicsInterface(file_path)
    pe_main_interface_traffic_data.get_pe_physics_interface_sheet()
    
    # 生成ce数据
    ce_interface_traffic_data = CEDevice(file_path)
    ce_interface_traffic_data.get_ce_device_interface_sheet()

    # 生成acc数据
    acc_line_traffic_data = AccLine(file_path)
    acc_line_traffic_data.get_ce_device_interface_sheet()
    
    # 生成CE物理接口多业务业务接口数据
    ce_main_interface_data = CEMainInterface(file_path)
    ce_main_interface_data.get_ce_physics_interface_sheet()

def pts_insert_data():
    # physical_node_line 数据插入数据库
    insert_node_line_traffic = PNodeLineInsertDatabase(mysql_info)
    insert_node_line_traffic.get_files()

    # logical_node_line 数据插入数据库
    insert_logical_node_line_traffic = LNodeLineInsertDatabase(mysql_info)
    insert_logical_node_line_traffic.get_file()

    # pe 数据插入数据库
    insert_pe_physics_interface_traffic = PEMainInterfaceInsertDatabase(mysql_info)
    insert_pe_physics_interface_traffic.get_file()
    
    # ce 数据插入数据库
    insert_ce_device_interface_traffic = CEInsertDatabase(mysql_info)
    insert_ce_device_interface_traffic.get_file()
    
    # acc 数据插入数据库
    insert_acc_line_traffic = AccLineInsertDatabase(mysql_info)
    insert_acc_line_traffic.get_file()
    
    # CE物理接口多业务业务接口数据插入数据库
    insert_ce_main_interface_sql = CEMainInsertDatabase(mysql_info)
    insert_ce_main_interface_sql.get_file()
    

def main():

    # logging.info("PTS Service data transfer starts")
    # node_line_and_pe_device()
    # ce_device()
    # acc_line()
    # device_load()
    # logging.info("PTS Service Data transmission complete")
    
    logging.info("PTS Service data create starts !")
    pts_create_data()
    logging.info("PTS Service data create end !")
    
    logging.info("PTS Service data insert starts !")
    pts_insert_data()
    logging.info("PTS Service data insert end !")
    
    logging.info("PTS Service Load data insert starts !")
    device_load()
    logging.info("PTS Service Load data insert end !")
    
    
    
    
if __name__ == "__main__":
    main()

    
