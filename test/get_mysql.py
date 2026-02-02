import pymysql
import logging

# 设置日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def query_mysql():
    try:
        # 连接到 MySQL 数据库
        conn = pymysql.connect(
            host='192.168.12.51',  # 数据库主机地址
            user='root',  # 数据库用户名
            password='Root@123!@#',  # 数据库密码
            database='pts_oam_center_demo_2.1.0',  # 数据库名称
            charset='utf8mb4',  # 使用的字符集
            cursorclass=pymysql.cursors.DictCursor  # 结果以字典格式返回
        )

        # 创建一个游标对象
        cur = conn.cursor()

        # 执行查询
        sql_query = 'SELECT id FROM node_line where node_line_name = "Center互联网" ;'
        cur.execute(sql_query)

        # 获取所有行
        rows = cur.fetchall()

        # 遍历并打印每一行
        for row in rows:
            logging.info(row)

    except Exception as e:
        logging.error(f"Error occurred: {e}")
    finally:
        # 关闭游标和连接
        cur.close()
        conn.close()

# 调用函数执行查询
query_mysql()
