#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import random
import time
import datetime

class TrafficFlow(object):
    def __init__(self, data_file, sr_b, ds_b, st_t, ed_t, node_line_name, node_line_id):
        self.data_file = data_file
        self.sr_b = sr_b
        self.ds_b = ds_b
        self.st_t = st_t
        self.ed_t = ed_t
        self.node_line_name = node_line_name
        self.node_line_id = node_line_id
        self.traffic_flow = {self.node_line_name:{}}
        self.data_multiple_var = round(self.ds_b / self.sr_b, 4)
        
    @staticmethod
    def random_num(a, b):
        # 生成随机数
        return round(sum([random.uniform(a, b) for _ in range(3)]) / 3, 2)
    
    def all_day_traffic(self, day_traffic):
        # 流量数据整合
        self.traffic_flow[self.node_line_name].update(day_traffic)
    
    def get_traffic_flow(self):
        # 生成数据
        while True:
            i = 0
            list_data = open(self.data_file, "r", encoding="utf-8")
            for list_var in list_data:
                list_data_var = list_var.strip().split(" ")
                list_week_var = int(list_data_var[1]) / 288
                ltime = time.localtime(int(self.st_t))
                date_ymd = time.strftime("%Y-%m-%d", ltime)
                week = datetime.datetime.strptime(date_ymd, "%Y-%m-%d").weekday()
                num = self.random_num(0.85, 0.98)
                if int(week) == int(list_week_var):
                    if i == 0:
                        tx_traffic_day = None
                        rx_traffic_day = None
                    # for time_num in range(288):
                    while i < 288:
                        in_traffic_var = list_data_var[5]
                        out_traffic_var = list_data_var[9]
                        in_traffic = int(in_traffic_var) * float(num) * float(self.data_multiple_var)
                        out_traffic = int(out_traffic_var) * float(num) * float(self.data_multiple_var)
                        if tx_traffic_day is not None:
                            tx_traffic_day = "{}|{}".format(tx_traffic_day, str(int(out_traffic)))
                        else:
                            tx_traffic_day = str(int(out_traffic))
                        if rx_traffic_day is not None:
                            rx_traffic_day = "{}|{}".format(rx_traffic_day, str(int(in_traffic)))
                        else:
                            rx_traffic_day = str(int(in_traffic))
                        i = i + 1
                        if i < 287:
                            break
                    else:
                        one_day_traffic = {
                            str(int(self.st_t) * 1000):{
                            'node_line_id': self.node_line_id,
                            "tx_traffic": tx_traffic_day,
                            "rx_traffic": rx_traffic_day
                            # "tx_traffic": rx_traffic_day,
                            # "rx_traffic": tx_traffic_day
                            }
                        }
                        self.all_day_traffic(one_day_traffic)
                        self.st_t = int(self.st_t) + 86400
                        i = 0
                        break

            list_data.close()
            if int(self.st_t) > int(self.ed_t):
                break

    
    def generate_traffic(self):
        self.get_traffic_flow()
        return self.traffic_flow


class CETrafficFlow(object):
    def __init__(self, data_file, sr_b, ds_b, st_t, ed_t):
        self.data_file = data_file
        self.sr_b = sr_b
        self.ds_b = ds_b
        self.st_t = st_t
        self.ed_t = ed_t
        # self.node_line_name = node_line_name
        # self.node_line_id = node_line_id
        self.traffic_flow = {}
        self.data_multiple_var = round(self.ds_b / self.sr_b, 4)
        
    @staticmethod
    def random_num(a, b):
        # 生成随机数
        return round(sum([random.uniform(a, b) for _ in range(3)]) / 3, 2)
    
    def all_day_traffic(self, day_traffic):
        # 流量数据整合
        self.traffic_flow.update(day_traffic)
    
    def get_traffic_flow(self):
        # 生成数据
        while True:
            i = 0
            list_data = open(self.data_file, "r", encoding="utf-8")
            for list_var in list_data:
                list_data_var = list_var.strip().split(" ")
                list_week_var = int(list_data_var[1]) / 288
                ltime = time.localtime(int(self.st_t))
                date_ymd = time.strftime("%Y-%m-%d", ltime)
                week = datetime.datetime.strptime(date_ymd, "%Y-%m-%d").weekday()
                num = self.random_num(0.85, 0.98)
                if int(week) == int(list_week_var):
                    if i == 0:
                        tx_traffic_day = None
                        rx_traffic_day = None
                    # for time_num in range(288):
                    while i < 288:
                        in_traffic_var = list_data_var[5]
                        out_traffic_var = list_data_var[9]
                        in_traffic = int(in_traffic_var) * float(num) * float(self.data_multiple_var)
                        out_traffic = int(out_traffic_var) * float(num) * float(self.data_multiple_var)
                        if tx_traffic_day is not None:
                            tx_traffic_day = "{}|{}".format(tx_traffic_day, str(int(out_traffic)))
                        else:
                            tx_traffic_day = str(int(out_traffic))
                        if rx_traffic_day is not None:
                            rx_traffic_day = "{}|{}".format(rx_traffic_day, str(int(in_traffic)))
                        else:
                            rx_traffic_day = str(int(in_traffic))
                        i = i + 1
                        if i < 287:
                            break
                    else:
                        one_day_traffic = {
                            str(int(self.st_t) * 1000):{
                            # "tx_traffic": tx_traffic_day,
                            # "rx_traffic": rx_traffic_day
                            "tx_traffic": rx_traffic_day,
                            "rx_traffic": tx_traffic_day
                            }
                        }
                        self.all_day_traffic(one_day_traffic)
                        self.st_t = int(self.st_t) + 86400
                        i = 0
                        break

            list_data.close()
            if int(self.st_t) >= int(self.ed_t):
                break

    
    def generate_traffic(self):
        self.get_traffic_flow()
        return self.traffic_flow




