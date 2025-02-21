import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sql_querys import GET_COMPETE_DATA_SQL



__all__ = ["GetCompeteData", "ProcessServiceFee"]

"""
获取指定日期的竞对影院票房信息

Args:
    date (str): 日期，格式为'YYYY-MM-DD'
    database (sqlalchemy.engine.Engine): 数据库引擎

Returns:
    pd.DataFrame: 包含竞对影院编码、竞对影院名称、票房、人次、上座率和平均票价的DataFrame

"""
# 获取前一天的竞对票房，未模块化
def GetCompeteData(date, db_conn):
    df_compete_table = pd.DataFrame()
    field_list = {
        "cinema_code":"竞对影城编码", 
        "cinema_name":"竞对影院名称", 
        "bo":"票房", 
        "people":"人次", 
        "occupancy":"上座率", 
        "avg_price":"平均票价"
    }

    df_compete = db_conn.sql_to_dataframe(
        text(GET_COMPETE_DATA_SQL), 
        params = {"date": date}
    )
    
    if not df_compete.empty:
        df_compete["cinema_name"] = df_compete["cinema_name"].str.strip()
        df_compete["arrange_seat"] = np.round(df_compete["people"] * 100 / df_compete["occupancy"])
        df_compete.fillna(0, inplace = True)

        df_compete_table = pd.pivot_table(
            df_compete, 
            index = ["cinema_code", "cinema_name"], 
            values = ["bo", "people", "arrange_seat"], 
            aggfunc = {"bo":np.sum, "people":np.sum, "arrange_seat":np.sum}, 
            fill_value = 0, 
            margins = False
        ).reset_index()

        df_compete_table["occupancy"] = np.round(df_compete_table["people"] * 100/ df_compete_table["arrange_seat"], 2)
        df_compete_table["avg_price"] = np.round(df_compete_table["bo"] / df_compete_table["people"], 2)
        df_compete_table.drop(columns = ["arrange_seat"], axis = 1, inplace = True)
        df_compete_table.fillna(0, inplace = True)
        df_compete_table.rename(columns = field_list, inplace = True)
    
    return df_compete_table


def ProcessServiceFee(data_file, serv_fee_dict):

    columns = ["nationalId", "cinema_strName", "book_fee", "book_fee_jy", "fourD_serv_fee", "bedroom_serv_fee", \
    "special_fee_online", "special_fee_offline", "check_fee_no", "check_fee_normal_no", "check_fee_online_no"]

    df = pd.read_json(data_file, encoding = "utf-8-sig")
    for each_col in ["book_fee", "book_fee_jy", "fourD_serv_fee", "bedroom_serv_fee", \
    "special_fee_online", "special_fee_offline", "check_fee_no", "check_fee_normal_no", "check_fee_online_no"]:
        df[each_col] = df[each_col].astype(float)

    df["total_fee"] = df["book_fee"] + df["book_fee_jy"] + df["fourD_serv_fee"] + \
    df["bedroom_serv_fee"] + df["special_fee_online"] + df["special_fee_offline"] + \
    df["check_fee_online_no"]
    # 日数据
    if "showDate" in df.columns:
        print("日服务费数据")
        df = df[columns + ["showDate", "total_fee"]]
        df.rename(columns = {"nationalId": "cinema_code", "cinema_strName": "cinema_name"}, inplace = True)
        df["showDate"] = pd.to_datetime(df["showDate"], format = "%Y-%m-%d").dt.date
        show_date_data = df[["showDate"]].groupby("showDate").count()
        if len(show_date_data) == 1:
            show_date = show_date_data.index[0]
            df["year"] = show_date.year
            df["month"] = show_date.month
            df["op_date"] = show_date
            df.drop(columns = ["showDate"], axis = 1, inplace = True)
            df_serv_fee = df
        else:
            print("日服务费数据超过1天，请检查并重新下载！")

    else:
        df = df[columns + ["showMonth", "total_fee"]]
        df.rename(columns = {"nationalId": "cinema_code", "cinema_strName": "cinema_name"}, inplace = True)
        df["showMonth"] = pd.to_datetime(df["showMonth"], format = "%Y-%m-%d")
        show_month_data = df[["showMonth"]].groupby("showMonth").count()
        if len(show_month_data) == 1:
            print("月服务费数据")
            show_month = show_month_data.index[0]
            df["year"] = show_month.year
            df["month"] = show_month.month
            df.drop(columns = ["showMonth"], axis = 1, inplace = True)
            df_serv_fee = df
        else:
            print("年服务费数据")
            df.rename(columns = {"nationalId": "cinema_code", "cinema_strName": "cinema_name"}, inplace = True)
            table = pd.pivot_table(
                df, 
                index = ["cinema_code", "cinema_name"], 
                values = ["book_fee", "book_fee_jy", "fourD_serv_fee", "bedroom_serv_fee", "special_fee_online", \
                "special_fee_offline", "check_fee_no", "check_fee_normal_no", "check_fee_online_no", "total_fee"], 
                aggfunc = np.sum, 
                fill_value = 0, 
                margins = False
            )
            table.reset_index(inplace = True)
            table["year"] = show_month_data.index[0].year
            df_serv_fee = table

    # 保存在字典中，不通过return返回
    serv_fee_dict["serv_fee"] = df_serv_fee