import pymysql
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from ftplib import FTP
import datetime
import re
import os


__all__ = ["DataBase", "FTPData"]


class DataBase:
    def __init__(self, host, user, passwd, db, port = 3306, charset = "utf8"):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port
        self.charset = charset
        self.connect = None
        self.engine = None

    """
    建立数据库连接

    Args:
        无

    Returns:
        pymysql.connections.Connection: 连接对象

    """
    def build_connect(self):
        self.connect = pymysql.connect(
            host = self.host, 
            port = self.port, 
            user = self.user, 
            passwd = self.passwd, 
            db = self.db, 
            charset = self.charset
        )

    """
    创建数据库引擎

    Args:
        无

    """
    def create_engine(self):
        self.engine = create_engine("mysql+pymysql://%s:%s@%s:%d/%s?charset=%s" % (self.user, self.passwd, self.host, self.port, self.db, self.charset))

    """
    将SQL查询结果转换为DataFrame对象

    Args:
        sql (str): SQL查询语句

    Returns:
        pd.DataFrame: 查询结果对应的DataFrame对象

    """
    def sql_to_dataframe(self, sql, params = None):
        data = None
        try:
            if self.engine is not None:
                data = pd.read_sql(sql, self.engine, params = params)
            else:
                data = pd.read_sql(sql, self.connect, params = params)
        except Exception as e:
            print(e)
        df = pd.DataFrame(data)
        return df

    """
    将pandas DataFrame对象转换为表格数据并插入到MySQL数据库中。

    Args:
        dataframe: pandas DataFrame对象，包含要插入的数据。
        table: str类型，表示要插入数据的MySQL表名。
        mode: str类型，可选值为"insert"或"update"，表示插入或更新数据。默认为"insert"。

    Returns:
        None

    """
    def dataframe_to_table(self, dataframe, table, mode = "insert"):
        today = datetime.date.today()
        cursor = self.connect.cursor()
        columns = dataframe.columns.tolist()
        # 需要 将之前数据清除
        if mode == "update":
            sql_to_delete = ""
            year = dataframe["year"][0]
            # 判断属于哪个日期的表
            if "op_date" in columns:
                op_date, month = dataframe["op_date"][0].dt.date, dataframe["month"][0]
                sql_to_delete = "delete from %s where year = '%s' and month = '%s' and op_date = '%s'" % (table, year, month, op_date)
            else:
                if "month" in columns:
                    month = dataframe["month"][0]
                    sql_to_delete = "delete from %s where year ='%s' and month = '%s'" % (table, year, month)
                else:
                    sql_to_delete = "delete from %s where year = '%s'" % (table, year)

            cursor.execute(sql_to_delete)

        fields = ",".join(columns) + ",update_time"
        data = np.array(dataframe).tolist()
        for each_data in data:
            string_data = str(each_data)[1:-1] + ",\'" + str(today) + "\'"
            sql_to_insert = "insert %s(%s) values (%s)" % (table, fields, string_data)
            cursor.execute(sql_to_insert)

        self.connect.commit()
        cursor.close()
    
    """
    关闭数据库连接。

    Args:
        无参数。

    Returns:
        无返回值。

    """
    def close_connect(self):
        self.connect.close()


class FTPData:
    def __init__(self, host, user, passwd, port = 21, timeout = 30, date = datetime.date.today() - datetime.timedelta(days = 1)):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.port = port
        self.timeout = timeout
        self.ftp = FTP()
        self.date = date
        self.dataframe = None

    """
    建立FTP连接并登录。

    Args:
        无

    Returns:
        无

    """
    def build_connect(self):
        self.ftp.connect(
            host = self.host, 
            port = self.port, 
            timeout = self.timeout
        )
        self.ftp.login(
            user = self.user, 
            passwd = self.passwd
        )
        self.ftp.set_pasv(False)

    """
    从FTP服务器获取指定日期的SessionRevenue数据，并返回pandas DataFrame对象。

    Args:
        date (str): 需要获取数据的日期，格式为YYYY-MM-DD。

    Returns:
        pd.DataFrame: 包含SessionRevenue数据的pandas DataFrame对象。

    Raises:
        无。

    """
    def fetch_data(self):
        lst = self.ftp.nlst()
        target_file = "SessionRevenue_"+ str(self.date).replace("-", "") +".csv"
        for each_file in lst:
            if re.match(target_file, each_file):
                file_handle = open(target_file, "wb+")
                self.ftp.retrbinary("RETR " + target_file, file_handle.write)
                file_handle.close()

        self.ftp.quit()
        dataframe = pd.read_csv(target_file)
        os.remove(target_file)
        self.dataframe = dataframe

    """
    处理数据，返回包含所需信息的DataFrame

    Args:
        self (object): 对象本身
        dataframe (pd.DataFrame): 包含场次信息的DataFrame

    Returns:
        pd.DataFrame: 包含所需信息的DataFrame

    """
    def process_data(self, db_conn):
        self.dataframe["场次时间"] = pd.to_datetime(self.dataframe["场次时间"])
        self.dataframe["date"] = pd.to_datetime(datetime.datetime(self.date.year, self.date.month, self.date.day, 6))
        self.dataframe["day_diff"] = (self.dataframe["场次时间"] - self.dataframe["date"]).dt.days
        self.dataframe = self.dataframe[self.dataframe["day_diff"].isin([0])]
        self.dataframe = self.dataframe[self.dataframe["场次状态"].isin(["开启"])]
        self.dataframe["影片"].replace("（.*?）\s*|\(.*?\)\s*|\s*", "", regex = True, inplace = True)
        
        db_conn.build_connect()
        dataframe_cinema_code = db_conn.sql_to_dataframe(
            sql = "select vista_cinema_name,cinema_code from jycinema_info"
        )

        table = pd.pivot_table(
            self.dataframe, 
            index = ["影院"], 
            values = ["票房","人数","总座位数"], 
            aggfunc = {"票房":np.sum,"人数":np.sum,"总座位数":np.sum}, 
            fill_value = 0, 
            margins = False
        )
        table = pd.DataFrame(table.reset_index())
        table["上座率"] = np.round((table["人数"] / table["总座位数"]) * 100, 2)
        table["平均票价"] = np.round(table["票房"] / table["人数"], 2)
        table = pd.merge(
            left = table, 
            right = dataframe_cinema_code, 
            left_on = "影院", 
            right_on = "vista_cinema_name", 
            how = "left"
        )
        table.rename(columns = {"cinema_code":"影城编码", "影院":"影院名称", "人数":"人次"}, inplace = True)
        table.drop(columns = ["总座位数", "vista_cinema_name"], axis = 1, inplace = True)
        table = table.reindex(columns = ["影城编码", "影院名称", "票房", "人次", "上座率", "平均票价"])
        none_code_cinema = table[table["影城编码"].isnull()]["影院名称"].tolist()
        if len(none_code_cinema) > 0:
            print("以下影城未匹配到编码，请重新检查\n")
            print(",".join(none_code_cinema))

        self.dataframe = table
        db_conn.close_connect()

    def save_dataframe(self):
        return self.dataframe
