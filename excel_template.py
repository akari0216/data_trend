import os
import pandas as pd
from database import DataBase


sheet_list = [
    ["票房数据", ["影城编码","影院名称","票房","人次","上座率","平均票价"]], 
    ["卖品数据", ["影城编码","影院名称","卖品营业额","卖品毛利润"]], 
    ["竞对票房", ["竞对影城编码","竞对影院名称","票房","人次","上座率","平均票价"]]
]
field_dict = {"影城编码":"cinema_code","影院名称":"cinema_name","票房":"bo","人次":"people","上座率":"occupancy","平均票价":"avg_price","渠道":"online_class",\
              "公司编号":"finance_cinema_code","公司名称":"cinema_name","电影票类别":"ticket_class","卖品营业额":"consession_amount","卖品毛利润":"consession_profit",\
              "竞对影城编码":"compete_cinema_code","竞对影院名称":"compete_cinema_name","年份":"year","月份":"month","日期":"op_date"}

# 下载、上传日月年数据模板
class ExcelTemplate:
    """
    初始化函数

    Args:
        path (str): 文件路径
        child_window_func (function): 子窗口回调函数
        output_texts (list): 输出文本列表
        writer_name (str): excel表名
        attach_cols (list): 附加列列表
        sheet_list (list): 工作表列表
        get_data_func (function): 获取数据回调函数
        db_conn (object): 数据库连接对象

    Returns:
        None
    """
    def __init__(self, path, child_window_func, output_texts, writer_name, attach_cols, sheet_list, get_data_func):
        self.path = path
        self.child_window = child_window_func
        self.output_contents = output_texts
        self.writer_name = writer_name
        self.attach_cols = attach_cols
        # sheet_list： 列表，每个元素为元组，元组第一个元素为工作表名，第二个元素为列名列表
        # 例如：sheet_list = [(sheet_name1, [cols]), (sheet_name2, [cols])]
        self.sheet_list = sheet_list
        self.get_data_func = get_data_func

    """
    下载Excel文件

    Args:
        无

    Returns:
        无返回值，函数将生成Excel文件并保存到指定路径

    """
    def download_excel(self):
        os.chdir(self.path)
        child_root, output = self.child_window_func()
        output(self.output_texts[0])
        writer = pd.ExcelWriter(self.writer_name)
        for each_sheet, each_cols in self.sheet_list:
            each_cols.extend(self.attach_cols)
            df = pd.DataFrame(columns = each_cols)
            df.to_excel(writer, sheet_name = each_sheet, header = True, index = False)

        # get_data_func()：仅日数据调用
        data = self.get_data_func()
        if data is None:
            pass
        else:
            df_data, df_compete_data = data
            df_data.to_excel(writer, sheet_name = "票房数据", header = True, index = False)
            df_compete_data.to_excel(writer, sheet_name = "竞对票房", header = True, index = False)

        writer.save()
        output(self.output_texts[1])
        child_root.mainloop()

    """
    将Excel文件中的数据上传至数据库中对应表格。

    Args:
        field_dict (dict): Excel列名与数据库列名对应的字典。
        sort_by (str): 数据排序的列名。
        ascending (bool): 数据排序是否升序。
        db_conn (object): 数据库连接对象。
        database_table_list (list): Excel表格对应的数据库表格列表。
        sql_data_check (str): 用于查询数据库中是否已存在对应数据的SQL语句模板。

    Returns:
        None

    """
    def upload_excel(self, field_dict, sort_by, ascending, db_conn, database_table_list, sql_data_check):
        os.chdir(self.path)
        child_root, output = self.child_window_func()
        output(self.output_texts[0])
        db_conn.build_connect()
        # 注意到sheet_list有3个表，需要插入到数据库中对应的3个table
        for (each_sheet, each_cols), each_table in zip(self.sheet_list, database_table_list):
            df_sheet = pd.read_excel(self.writer_name, sheet_name = each_sheet)
            if len(df_sheet) != 0:
                df_sheet.rename(columns =  field_dict, inplace = True)
                df_sheet.sort_values(by = sort_by, ascending = ascending, inplace = True)
                # 要取得df_sheet的时间参数，以此判断是否插入数据
                # 不同时间维度下需要的时间参数个数和具体内容不同·
                year, month, check_date, check_month, check_date = None, None, None, None, None
                date_list, month_list, date_list = [], [], []
                if "daily" in each_table:
                    year, month = df_sheet["year"][0], df_sheet["month"][0]
                    date_list = df_sheet["op_date"].drop_duplicates().tolist()
                    check_date = date_list[0]
                    sql_data_check = sql_data_check % (each_table, year, month, check_date)
                else:
                    if "month" in each_table:
                        year, month_list = df_sheet["year"][0], month_list = df_sheet["month"].drop_duplicates().tolist()
                        check_month = month_list[0]
                        sql_data_check = sql_data_check % (each_table, year, check_month)
                    else:
                        year_list = df_sheet["year"].drop_duplicates().tolist()
                        check_year = year_list[0]
                        sql_data_check = sql_data_check % (each_table, check_year)

                data_check = db_conn.sql_to_dataframe(sql_data_check)
                if len(data_check) == 0:
                    db_conn.dataframe_to_table(df_sheet, each_table, mode = "insert")
                else:
                    if "daily" in each_table:
                        for each_date in date_list:
                            each_df = df_sheet[df_sheet["year"].isin([year]) & df_sheet["month"].isin([month]) & df_sheet["op_date"].isin([each_date])]
                            db_conn.dataframe_to_table(each_df, each_table, mode = "update")
                    else:
                        if "month" in each_table:
                            for each_month in month_list:
                                each_df = df_sheet[df_sheet["year"].isin([year]) & df_sheet["month"].isin([each_month])]
                                db_conn.dataframe_to_table(each_df, each_table, mode = "update")
                        else:
                            for each_year in year_list:
                                each_df = df_sheet[df_sheet["year"].isin([each_year])]
                                db_conn.dataframe_to_table(each_df, each_table, mode = "update")

        db_conn.close_connect()
        output(self.output_texts[1])
        child_root.mainloop()