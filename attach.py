import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text



__all__ = ["get_compete_bo"]

"""
获取指定日期的竞对影院票房信息

Args:
    date (str): 日期，格式为'YYYY-MM-DD'
    database (sqlalchemy.engine.Engine): 数据库引擎

Returns:
    pd.DataFrame: 包含竞对影院编码、竞对影院名称、票房、人次、上座率和平均票价的DataFrame

"""
# 获取前一天的竞对票房，未模块化
def get_compete_bo(date, database):
    df_compete_table = pd.DataFrame()
    field_list = {
        "cinema_code":"竞对影城编码", 
        "cinema_name":"竞对影院名称", 
        "bo":"票房", 
        "people":"人次", 
        "occupancy":"上座率", 
        "avg_price":"平均票价"
    }
    sql_query  = text("""
    SELECT cinema_code, cinema_name, movie_name, bo, people, occupancy 
    FROM topcdb_compete_cinema_daily
    WHERE op_date = :date
    """)
    df_compete = database.read_sql(
        sql_query, 
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

