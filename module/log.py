#!/usr/bin/python
# -*- coding:utf-8 -*-

import logging
import time
from pathlib import Path
from logging.handlers import RotatingFileHandler

class Log:
    '''
    封装后的logging
    '''

    def __init__(self, logger_name=None, 
                 log_cate='PTS_Traffic', 
                 log_level=logging.INFO, 
                #  max_bytes=5*1024*1024, 
                 backup_count=5):
        '''
        指定保存日志的文件路径，日志级别，以及调用文件
        将日志存入到指定的文件中
        '''
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(log_level)

        # 防止重复添加处理器
        if not self.logger.handlers:
            # 获取当前日期
            self.log_time = time.strftime("%Y_%m_%d")

            # 定义日志目录
            log_dir = Path(__file__).resolve().parent.parent / 'log'
            log_dir.mkdir(parents=True, exist_ok=True)

            # 定义日志文件路径
            log_name = log_dir / f"{log_cate}.log"

            # 创建文件处理器，设置文件大小限制和备份文件数量
            fh = RotatingFileHandler(log_name, mode='a', backupCount=backup_count, encoding='utf-8')
            fh.setLevel(log_level)
            # fh.setLevel(logging.CRITICAL)
            
            # 创建控制台处理器
            ch = logging.StreamHandler()
            # ch.setLevel(log_level)
            ch.setLevel(logging.CRITICAL)
            

            # 定义日志格式，包括时间、日志级别、文件名、行号和消息
            formatter = logging.Formatter(
                '[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s'
            )
            fh.setFormatter(formatter)
            ch.setFormatter(formatter)

            # 添加处理器到logger
            self.logger.addHandler(fh)
            self.logger.addHandler(ch)

    def getlog(self):
        return self.logger

    def clear_handlers(self):
        '''清除所有处理器'''
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
            handler.close()

# 使用示例
# if __name__ == "__main__":
#     log = Log(__name__).getlog()
#     log.info("This is an info message")
#     log.debug("This is a debug message")
#     log.error("This is an error message")
