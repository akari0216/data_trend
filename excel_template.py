import os
import numpy as np
import pandas as pd
from database import DataBase
from collections import OrderedDict
from attach import GetCompeteData
from sqlalchemy import text
from sql_querys import GET_COMPETE_SERV_FEE_SQL


field_dict = {"影城编码":"cinema_code","影院名称":"cinema_name","票房":"bo","人次":"people","上座率":"occupancy","平均票价":"avg_price","渠道":"online_class",\
              "公司编号":"finance_cinema_code","公司名称":"cinema_name","电影票类别":"ticket_class","卖品营业额":"consession_amount","卖品毛利润":"consession_profit",\
              "竞对影城编码":"compete_cinema_code","竞对影院名称":"compete_cinema_name","年份":"year","月份":"month","日期":"op_date"}



# 下载、上传日月年数据模板
class ExcelTemplate:
    """
    初始化函数

    Args:
        path (str): 文件路径
        writer_name (str): excel表名
        attach_cols (list): 附加列列表
        sheet_list (list): 工作表列表
        get_data_func (function): 获取数据回调函数
        db_conn (object): 数据库连接对象

    Returns:
        None
    """
    def __init__(
        self, 
        path, 
        op_date, 
        writer_name, 
        attach_cols, 
        db_conn_from, 
        db_conn_to, 
        db_table_list, 
        ftp = False, 
        ftp_conn = None, 
        date_type = "daily"
        ):
        self.path = path
        self.op_date = op_date
        self.writer_name = writer_name
        self.attach_cols = attach_cols
        # sheet_list调整为OrderedDict
        self.sheet_list = OrderedDict()
        self.db_conn_from = db_conn_from
        self.db_conn_to = db_conn_to
        self.db_table_list = db_table_list
        self.ftp = ftp
        self.ftp_conn = ftp_conn
        self.date_type = date_type


    def init_sheet_list(self):
        sheet_names = ["票房数据", "卖品数据", "竞对票房", "直营服务费", "竞对服务费"]
        field_lists = [
            ["影城编码", "影院名称", "票房", "人次", "上座率", "平均票价"], 
            ["影城编码", "影院名称", "卖品营业额", "卖品毛利润"], 
            ["竞对影城编码", "竞对影院名称", "票房", "人次", "上座率", "平均票价"], 
            ["cinema_code", "cinema_name", "book_fee", "book_fee_jy", "fourD_serv_fee", "bedroom_serv_fee", \
        "special_fee_online", "special_fee_offline", "check_feze_no", "check_fee_normal_no", "check_fee_online_no", "total_fee"], 
            ["compete_cinema_code", "compete_cinema_name", "total_fee"]
        ]
        for each_sheet, each_fields in zip(sheet_names, field_lists):
            each_fields.extend(self.attach_cols)
            self.sheet_list[each_sheet] = each_fields

    """
    下载Excel文件

    Args:
        无

    Returns:
        无返回值，函数将生成Excel文件并保存到指定路径

    """
    def download_excel(self, serv_fee_dict):
        self.init_sheet_list()
        self.db_conn_from.create_engine()

        os.chdir(self.path)
        df_consession = pd.DataFrame(columns = self.sheet_list["卖品数据"])
        consession_path = "C:\\Users\\Administrator\\Desktop\\卖品、影厅服务费数据模板"
        if "数据模板-新版.xlsx" in os.listdir(consession_path):
            df_consession = pd.read_excel(consession_path + "\\数据模板-新版.xlsx", sheet_name = "卖品数据")
            df_consession = df_consession[["影城编码", "影院名称", "卖品营业额", "卖品毛利润"]]
            df_consession["影城编码"] = df_consession["影城编码"].astype(str)
        else:
            print("卖品数据缺失！")

        df_bo = pd.DataFrame(columns = self.sheet_list["票房数据"])
        df_compete_bo = pd.DataFrame(columns = self.sheet_list["竞对票房"])
        df_compete_serv_fee = pd.DataFrame(columns = self.sheet_list["竞对服务费"])

        if self.date_type == "daily":
            if self.ftp:
                self.ftp_conn.build_connect()
                self.ftp_conn.fetch_data()
                self.ftp_conn.process_data(self.db_conn_to)
                df_bo = self.ftp_conn.save_dataframe()

            df_get_compete_data = GetCompeteData(
                date = self.op_date,
                db_conn = self.db_conn_from,
            )
            df_compete_bo[df_get_compete_data.columns] = df_get_compete_data

            df_compete_serv_fee_sql = self.db_conn_from.sql_to_dataframe(
                sql = text(GET_COMPETE_SERV_FEE_SQL), 
                params = {"date": self.op_date}
            )
            df_compete_serv_fee_sql.rename(
                columns = {"cinema_code": "compete_cinema_code", "cinema_name": "compete_cinema_name"},
                inplace = True
            )
            df_compete_serv_fee_sql = pd.pivot_table(
                df_compete_serv_fee_sql, 
                index = ["compete_cinema_code", "compete_cinema_name"], 
                values = ["fee"], 
                aggfunc = np.sum, 
                fill_value = 0, 
                margins = False
            )
            df_compete_serv_fee_sql.reset_index(inplace = True)
            df_compete_serv_fee_sql["fee"] = df_compete_serv_fee_sql["fee"].astype(float)
            df_compete_serv_fee_sql["fee"] = np.divide(df_compete_serv_fee_sql["fee"], 10000)
            df_compete_serv_fee_sql.rename(columns = {"fee": "total_fee"}, inplace = True)

            df_compete_serv_fee[df_compete_serv_fee_sql.columns] = df_compete_serv_fee_sql

        excel_writer = pd.ExcelWriter(self.writer_name)
        for each_sheet, each_df in zip(self.sheet_list.keys(), [df_bo, df_consession, df_compete_bo, serv_fee_dict["serv_fee"], df_compete_serv_fee]):
            if each_sheet != "直营服务费":
                if self.date_type == "daily":
                    each_df["年份"] = self.op_date.year
                    each_df["月份"] = self.op_date.month
                    each_df["日期"] = self.op_date
                elif self.date_type == "month":
                    each_df["年份"] = self.op_date.year
                    each_df["月份"] = self.op_date.month
                elif self.date_type == "year":
                    each_df["年份"] = self.op_date.year

            each_df.to_excel(excel_writer, sheet_name = each_sheet, index = False, header = True)
        
        excel_writer.save()

    """
    将Excel文件中的数据上传至数据库中对应表格。

    Args:
        field_dict (dict): Excel列名与数据库列名对应的字典。
        sort_by (str): 数据排序的列名。
        ascending (bool): 数据排序是否升序。
        db_conn (object): 数据库连接对象。
        self.db_table_list (list): Excel表格对应的数据库表格列表。
        sql_data_check (str): 用于查询数据库中是否已存在对应数据的SQL语句模板。

    Returns:
        None

    """
    def upload_excel(self, field_dict):
        os.chdir(self.path)
        self.db_conn_to.build_connect()
        # 注意到sheet_list有5个表，需要插入到数据库中对应的5个table
        for (each_sheet, each_cols), each_table in zip(self.sheet_list.items(), self.db_table_list):
            df_sheet = pd.read_excel(self.writer_name, sheet_name = each_sheet)
            if len(df_sheet) != 0:
                df_sheet.rename(columns =  field_dict, inplace = True)
                # 要取得df_sheet的时间参数，以此判断是否插入数据
                sql_data_check = ""
                if "日数据" in self.writer_name:
                    year, month, date = df_sheet["year"][0], df_sheet["month"][0], df_sheet["op_date"][0]
                    sql_data_check = """
                        SELECT * 
                        FROM %s 
                        WHERE year = %s AND month = %s AND op_date = %s
                    """
                    sql_data_check = sql_data_check % (each_table, year, month, date)
                    
                elif "月数据" in self.writer_name:
                    year, month = df_sheet["year"][0], df_sheet["month"][0]
                    sql_data_check = """
                    SELECT * 
                    FROM %s 
                    WHERE year = %s AND month = %s
                    """
                    sql_data_check = sql_data_check % (each_table, year, month)

                elif "年数据" in self.writer_name:
                    year = df_sheet["year"][0]
                    sql_data_check = """
                    SELECT * 
                    FROM %s 
                    WHERE year = %s
                    """
                    sql_data_check = sql_data_check % (each_table, year)

                data_check = self.db_conn_to.sql_to_dataframe(sql_data_check)
                if len(data_check) == 0:
                    self.db_conn_to.dataframe_to_table(df_sheet, each_table, mode = "insert")
                else:
                    self.db_conn_to.dataframe_to_table(df_sheet, each_table, mode = "update")

        self.db_conn_to.close_connect()