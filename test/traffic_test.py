#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
import random
import sys
import time
import datetime
import json
import os


class TrafficGenerator:
    def __init__(self, args):
        self.traffic = {
            "service_interface_code": args[7],
            "data_list": []
        }
        self.traffic_dir_file = os.path.join(
            "/home/liqing/project/python/agent/agent_run/agent_traffic_all/all_traffic", args[1])
        self.target_flow = int(args[2])
        self.data_flow = float(args[3])
        self.start_timestamp = int(args[4])
        self.stop_timestamp = int(args[5])
        self.data_file_path = args[6]
        self.data_multiple_var = round(self.target_flow / self.data_flow, 4)

    @staticmethod
    def random_num(a, b):
        # 生成随机数
        return round(sum([random.uniform(a, b) for _ in range(3)]) / 3, 2)

    def all_day_traffic(self, day_traffic):
        # 流量数据写入文件
        self.traffic["data_list"].append(day_traffic)

    def data_list(self):
        # 整理数据
        with open(self.data_file_path, "r", encoding="utf-8") as list_data:
            list_lines = list_data.readlines()

        start_times = self.start_timestamp
        while start_times <= self.stop_timestamp:
            ltime = time.localtime(start_times)
            date_ymd = time.strftime("%Y-%m-%d", ltime)
            week = datetime.datetime.strptime(date_ymd, "%Y-%m-%d").weekday()

            for list_var in list_lines:
                list_data_var = list_var.strip().split(" ")
                list_week_var = int(list_data_var[1]) / 288

                if int(week) == int(list_week_var):
                    num = self.random_num(0.85, 0.98)
                    in_traffic_var = int(list_data_var[5])
                    out_traffic_var = int(list_data_var[9])

                    tx_traffic_day = "|".join(
                        [str(int(out_traffic_var * num * self.data_multiple_var))] * 288)
                    rx_traffic_day = "|".join(
                        [str(int(in_traffic_var * num * self.data_multiple_var))] * 288)

                    one_day_traffic = {
                        "data_time": str(int(start_times) * 1000),
                        "monitor_period": "5min",
                        "tx_traffic": tx_traffic_day,
                        "rx_traffic": rx_traffic_day
                    }
                    self.all_day_traffic(one_day_traffic)
                    start_times += 86400
                    break

    def generate_traffic(self):
        self.data_list()
        with open(self.traffic_dir_file, "w", encoding="utf-8") as traffic_file:
            json.dump(self.traffic, traffic_file, ensure_ascii=True)


def main():
    args = sys.argv
    generator = TrafficGenerator(args)
    generator.generate_traffic()


if __name__ == "__main__":
    main()
