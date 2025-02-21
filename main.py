import datetime
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, bindparam
import os
import time
from database import DataBase, FTPData
from data_process import CinemaData, CityData
from collections import OrderedDict, defaultdict
from sql_querys import *
from attach import *
from components import *
from excel_template import *
from data_process import *
from mail_sender import *
import tkinter as tk
from tkinter import ttk
from tkinter import filedialog, messagebox
from tkinter.filedialog import askdirectory, askopenfilename
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from queue import Queue
import random
from logo1 import img1


# 时间显示
def ClockTick():
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    clock.config(text = current_time)
    root.after(1000, ClockTick)

    # 文件选择
def SelectFile():
    file_path_ = askopenfilename()
    if file_path_:
        print(file_path)
        file_path.set(file_path_)

# 路径选择 
def SelectPath():
    path_ = askdirectory()
    path.set(path_)

def download_daily_data(
    data_dict, 
    path, 
    op_date, 
    db_conn_from, 
    db_conn_to, 
    ftp_conn, 
    child_window, 
    serv_fee_dict
    ):
    child_root, output = child_window
    output("正在下载日数据...\n")

    daily_data = ExcelTemplate(
        path = path, 
        op_date = op_date,  
        writer_name = path + "\\%s日数据.xlsx" % (op_date), 
        attach_cols = ["年份", "月份", "日期"], 
        db_conn_from = db_conn_from, 
        db_conn_to = db_conn_to, 
        ftp = True, 
        ftp_conn = ftp_conn, 
        db_table_list = [
            "jycinema_data_daily", 
            "jycinema_consession_daily", 
            "compete_cinema_data_daily", 
            "jycinema_serv_fee_daily", 
            "compete_cinema_serv_fee_daily"
        ]
    )

    daily_data.download_excel(
        serv_fee_dict = serv_fee_dict
    )
    output("日数据下载完成！\n")

    data_dict["daily_data"] = daily_data
    child_root.mainloop()


def upload_daily_data(
    data_dict, 
    child_window, 
    field_dict
    ):
    child_root, output = child_window
    output("正在上传日数据...\n")
    daily_data = data_dict["daily_data"]
    daily_data.upload_excel(
        field_dict = field_dict
    )

    output("日数据上传完成!\n")
    child_root.mainloop()


def download_month_data(
    data_dict, 
    path, 
    op_date, 
    db_conn_from, 
    db_conn_to, 
    child_window, 
    serv_fee_dict
    ):
    child_root, output = child_window
    output("正在下载月数据...\n")

    month_data = ExcelTemplate(
        path = path, 
        op_date = op_date, 
        writer_name = path + "\\%s-%s月数据.xlsx" % (op_date.year, op_date.month), 
        attach_cols = ["年份", "月份"], 
        db_conn_from = db_conn_from, 
        db_conn_to = db_conn_to, 
        db_table_list = [
            "jycinema_data_month", 
            "jycinema_consession_month", 
            "compete_cinema_data_month", 
            "jycinema_serv_fee_month", 
            "compete_cinema_serv_fee_month"
        ], 
        date_type = "month"
    )
    month_data.download_excel(
        serv_fee_dict = serv_fee_dict
    )
    data_dict["month_data"] = month_data
    
    output("月数据下载完成!\n")
    child_root.mainloop()


def upload_month_data(
    data_dict, 
    child_window, 
    field_dict
    ):
    child_root, output = child_window
    output("正在上传月数据...\n")
    month_data = data_dict["month_data"]
    month_data.upload_excel(
        field_dict = field_dict
    )

    output("月数据上传完成!\n")
    child_root.mainloop()


def download_year_data(
    data_dict, 
    path, 
    op_date, 
    db_conn_from, 
    db_conn_to, 
    child_window, 
    serv_fee_dict
    ):
    child_root, output = child_window
    output("正在下载年数据...\n")

    year_data = ExcelTemplate(
        path = path, 
        op_date = op_date, 
        writer_name = path + "\\%s年数据.xlsx" % (op_date.year), 
        attach_cols = ["年份"], 
        db_conn_from = db_conn_from, 
        db_conn_to = db_conn_to, 
        db_table_list = [
            "jycinema_data_year", 
            "jycinema_consession_year", 
            "compete_cinema_data_year", 
            "jycinema_serv_fee_year", 
            "compete_cinema_serv_fee_year"
        ], 
        date_type = "year"
    )
    year_data.download_excel(
        serv_fee_dict = serv_fee_dict
    )
    data_dict["year_data"] = year_data

    output("年数据下载完成!\n")
    child_root.mainloop()



def upload_year_data(
    data_dict, 
    child_window, 
    field_dict
):
    child_root, output = child_window
    output("正在上传年数据...\n")
    year_data = data_dict["year_data"]
    year_data.upload_excel(
        field_dict = field_dict
    )

    output("年数据上传完成!\n")
    child_root.mainloop()

def random_list_split(list_data, n):
    random.shuffle(list_data)
    size, remainder = divmod(len(list_data), n)
    
    result = [[] for _ in range(n)]
    cnt, start_idx = 0, 0
    # 分配元素到子列表
    for i in range(n):
        if cnt < remainder:
            end_idx = start_idx + size + 1
            cnt += 1
        else:
            end_idx = start_idx + size
        result[i] = list_data[start_idx:end_idx]
        start_idx = end_idx

    return result

def thread_done_callback(future, thread_queue, thread_id, output, time_st):
    if future.done():
        thread_queue.get()
        output("线程id:%s 已完成!\n" % (thread_id))
        time_end = time.time()
        output("当前总耗时: %.3f秒\n" % (time_end - time_st))
    if thread_queue.empty():
        output("所有线程已完成!\n")

def create_city_excel(
    city_list, 
    db_conn, 
    op_date, 
    output, 
    thread_id, 
    thread_queue, 
    city_list_sql = CITY_LIST_SQL, 
    cinema_list_except_combine_sql = CINEMA_LIST_EXCEPT_COMBINE_SQL, 
    combine_cinema_list_sql = COMBINE_CINEMA_LIST_SQL, 
    jy_info_sql = JY_INFO_SQL, 
    compete_info_sql = COMPETE_INFO_SQL, 
    jy_data_year_sql = JY_DATA_YEAR_SQL, 
    jy_data_month_sql = JY_DATA_MONTH_SQL, 
    jy_data_daily_sql = JY_DATA_DAILY_SQL, 
    compete_data_year_sql = COMPETE_DATA_YEAR_SQL, 
    compete_data_month_sql = COMPETE_DATA_MONTH_SQL, 
    compete_data_daily_sql = COMPETE_DATA_DAILY_SQL, 
    jy_market_ratio_revise_sql = JY_MARKET_RATIO_REVISE_SQL, 
    jy_market_ratio_target_sql = JY_MARKET_RATIO_TARGET_SQL, 
    jy_bo_target_sql = JY_BO_TARGET_SQL, 
    jy_serv_fee_year_sql = JY_SERV_FEE_YEAR_SQL, 
    jy_serv_fee_month_sql = JY_SERV_FEE_MONTH_SQL, 
    jy_serv_fee_daily_sql = JY_SERV_FEE_DAILY_SQL, 
    compete_serv_fee_year_sql = COMPETE_SERV_FEE_YEAR_SQL, 
    compete_serv_fee_month_sql = COMPETE_SERV_FEE_MONTH_SQL, 
    compete_serv_fee_daily_sql = COMPETE_SERV_FEE_DAILY_SQL, 
    jy_consession_daily_sql = JY_CONSESSION_DAILY_SQL, 
    jy_consession_month_sql = JY_CONSESSION_MONTH_SQL, 
    jy_consession_year_sql = JY_CONSESSION_YEAR_SQL, 
    cinema_code_from_combine_code_sql = CINEMA_CODE_FROM_COMBINE_CODE_SQL, 
    combine_name_sql = COMBINE_NAME_SQL, 
    jy_city_market_ratio_revise_sql = JY_CITY_MARKET_RATIO_REVISE_SQL, 
    jy_city_market_ratio_target_sql = JY_CITY_MARKET_RATIO_TARGET_SQL, 
    jy_city_bo_target_sql = JY_CITY_BO_TARGET_SQL
    ):
    thread_queue.put(thread_id)
    for each_city in city_list:
        date_name = str(op_date).replace("-", "")
        city_excel_writer_name = "%s同城每日票房卖品数据(至%s)(含服务费).xlsx" % (each_city, date_name)
        output("\n线程id:%s 正在生成%s同城及其影城走势表...\n" % (thread_id, each_city))

        city_data = CityData(db_conn, each_city, city_excel_writer_name, op_date)
        df_cinema_list = db_conn.sql_to_dataframe(
            sql = text(CINEMA_LIST_EXCEPT_COMBINE_SQL), 
            params = {"city": each_city}
        )
        cinema_list = df_cinema_list["cinema_code"].tolist()
        cinema_name_list = df_cinema_list["cinema_name"].tolist()

        # 先遍历同城每个影城，再检测是否有合并影城，最终将CityData的实例生成excel文件
        for each_cinema_code, each_cinema_name in zip(cinema_list, cinema_name_list):
            city_data.add_data_to_cinema_code_list(each_cinema_code)
            city_data.add_data_to_cinema_name_list(each_cinema_name)
            excel_writer_name = "%s每日票房卖品数据(至%s)(含服务费).xlsx" % (each_cinema_name, date_name)
            output("线程id:%s 正在生成%s影城走势表...\n" % (thread_id, each_cinema_name))
            each_cinema_data = CinemaData(
                db_conn = db_conn,
                op_date = op_date, 
                excel_writer_name = excel_writer_name, 
                cinema_code = each_cinema_code
            )
            each_cinema_data.create_cinema_excel(
            jy_info_sql = JY_INFO_SQL, 
            compete_info_sql = COMPETE_INFO_SQL, 
            jy_data_year_sql = JY_DATA_YEAR_SQL, 
            jy_data_month_sql = JY_DATA_MONTH_SQL, 
            jy_data_daily_sql = JY_DATA_DAILY_SQL, 
            compete_data_year_sql = COMPETE_DATA_YEAR_SQL, 
            compete_data_month_sql = COMPETE_DATA_MONTH_SQL, 
            compete_data_daily_sql = COMPETE_DATA_DAILY_SQL, 
            jy_market_ratio_revise_sql = JY_MARKET_RATIO_REVISE_SQL, 
            jy_market_ratio_target_sql = JY_MARKET_RATIO_TARGET_SQL, 
            jy_bo_target_sql = JY_BO_TARGET_SQL, 
            jy_serv_fee_year_sql = JY_SERV_FEE_YEAR_SQL, 
            jy_serv_fee_month_sql = JY_SERV_FEE_MONTH_SQL, 
            jy_serv_fee_daily_sql = JY_SERV_FEE_DAILY_SQL, 
            compete_serv_fee_year_sql = COMPETE_SERV_FEE_YEAR_SQL, 
            compete_serv_fee_month_sql = COMPETE_SERV_FEE_MONTH_SQL, 
            compete_serv_fee_daily_sql = COMPETE_SERV_FEE_DAILY_SQL, 
            jy_consession_daily_sql = JY_CONSESSION_DAILY_SQL, 
            jy_consession_month_sql = JY_CONSESSION_MONTH_SQL, 
            jy_consession_year_sql = JY_CONSESSION_YEAR_SQL, 
            )

            weights = each_cinema_data.save_weights()
            city_data.add_data_to_weight_list(weights)

            dataframe_list = each_cinema_data.save_dataframe_list()
            df_bo, df_people, df_occupancy, df_avg_price, df_consession = dataframe_list
            city_data.add_data_to_df_bo_list(df_bo)
            city_data.add_data_to_df_people_list(df_people)
            city_data.add_data_to_df_occupancy_list(df_occupancy)
            city_data.add_data_to_df_avg_price_list(df_avg_price)
            city_data.add_data_to_df_consession_list(df_consession)

        # 检查是否有合并影城，如果有，则生成合并影城的excel文件
        df_combine_cinema_list = db_conn.sql_to_dataframe(
            sql = text(COMBINE_CINEMA_LIST_SQL), 
            params = {"city": each_city}
        )
        # 目前指定合并影城数量上限为1个
        if len(df_combine_cinema_list) == 1:
            combine_code = df_combine_cinema_list["combine_code"].tolist()[0]
            combine_name = df_combine_cinema_list["combine_name"].tolist()[0]
            city_data.add_data_to_cinema_code_list(combine_code)
            city_data.add_data_to_cinema_name_list(combine_name)
            excel_writer_name = "%s每日票房卖品数据(至%s)(含服务费).xlsx" % (combine_name, date_name)
            output("线程id:%s 正在生成%s合并影城走势表...\n" % (thread_id, combine_name))
            combine_cinema_data = CinemaData(
                db_conn = db_conn, 
                op_date = op_date, 
                excel_writer_name = excel_writer_name, 
                combine_code = combine_code
            )
            combine_cinema_data.create_combine_cinema_excel(
                jy_info_sql = JY_INFO_SQL, 
                compete_info_sql = COMPETE_INFO_SQL, 
                cinema_code_from_combine_code_sql = CINEMA_CODE_FROM_COMBINE_CODE_SQL, 
                combine_name_sql = COMBINE_NAME_SQL, 
                jy_data_year_sql = JY_DATA_YEAR_SQL, 
                jy_data_month_sql = JY_DATA_MONTH_SQL, 
                jy_data_daily_sql = JY_DATA_DAILY_SQL, 
                compete_data_year_sql = COMPETE_DATA_YEAR_SQL, 
                compete_data_month_sql = COMPETE_DATA_MONTH_SQL, 
                compete_data_daily_sql = COMPETE_DATA_DAILY_SQL, 
                jy_market_ratio_revise_sql = JY_MARKET_RATIO_REVISE_SQL, 
                jy_market_ratio_target_sql = JY_MARKET_RATIO_TARGET_SQL, 
                jy_bo_target_sql = JY_BO_TARGET_SQL, 
                jy_serv_fee_year_sql = JY_SERV_FEE_YEAR_SQL, 
                jy_serv_fee_month_sql = JY_SERV_FEE_MONTH_SQL, 
                jy_serv_fee_daily_sql = JY_SERV_FEE_DAILY_SQL, 
                compete_serv_fee_year_sql = COMPETE_SERV_FEE_YEAR_SQL, 
                compete_serv_fee_month_sql = COMPETE_SERV_FEE_MONTH_SQL, 
                compete_serv_fee_daily_sql = COMPETE_SERV_FEE_DAILY_SQL, 
                jy_consession_daily_sql = JY_CONSESSION_DAILY_SQL, 
                jy_consession_month_sql = JY_CONSESSION_MONTH_SQL, 
                jy_consession_year_sql = JY_CONSESSION_YEAR_SQL,
            )
            weights = combine_cinema_data.save_weights()
            city_data.add_data_to_weight_list(weights)

            dataframe_list = combine_cinema_data.save_dataframe_list()
            df_bo, df_people, df_occupancy, df_avg_price, df_consession = dataframe_list

            city_data.add_data_to_df_bo_list(df_bo)
            city_data.add_data_to_df_people_list(df_people)
            city_data.add_data_to_df_occupancy_list(df_occupancy)
            city_data.add_data_to_df_avg_price_list(df_avg_price)
            city_data.add_data_to_df_consession_list(df_consession)

        # 生成同城报表
        output("线程id:%s 正在生成%s同城走势表...\n" % (thread_id, each_city))
        city_data.create_city_excel(
            jy_city_market_ratio_revise_sql = JY_CITY_MARKET_RATIO_REVISE_SQL, 
            jy_city_market_ratio_target_sql = JY_CITY_MARKET_RATIO_TARGET_SQL, 
            jy_city_bo_target_sql = JY_CITY_BO_TARGET_SQL, 
        )


def create_all_excel(
    path, 
    db_conn, 
    op_date, 
    child_window, 
    thread_num = 1, 
    ):
    child_root, output = child_window
    output("已选择线程数：%s\n" % thread_num)
    time_st = time.time()

    output("正在生成所有同城影城走势表...\n")
    os.chdir(path)
    db_conn.create_engine()
    city_list = db_conn.sql_to_dataframe(
        sql = text(CITY_LIST_SQL), 
        params = None
    )["city"].tolist()
    # city_list = ["广州"]
    thread_city_list = random_list_split(city_list, thread_num)
    futures = []
    thread_queue = Queue()
    # with ... as executor会导致阻塞
    executor = ThreadPoolExecutor(max_workers = thread_num)
    for thread_id, each_city_list in enumerate(thread_city_list):
        print("线程id:", thread_id + 1, " 同城列表:", each_city_list)
        future = executor.submit(
            create_city_excel, 
            each_city_list, 
            db_conn, 
            op_date, 
            output, 
            thread_id + 1, 
            thread_queue
        )
        print("future running status:", future.running())
        callback_with_args = partial(
            thread_done_callback, 
            thread_queue = thread_queue, 
            thread_id = thread_id + 1, 
            output = output, 
            time_st = time_st
        )
        future.add_done_callback(callback_with_args)

    child_root.mainloop()


def sendmail_all_excel(
    path, 
    db_conn, 
    op_date, 
    child_window
    ):
    child_root, output = child_window
    os.chdir(path)
    output("正在发送所有影城/同城走势表...\n")
    time_st = time.time()
    date_name = str(op_date).replace("-", "")

    # 构造邮件发送器对象
    smtp_server = "smtp.exmail.qq.com"
    db_conn.create_engine()
    mail_sender_acc = db_conn.sql_to_dataframe(
        sql = text(MAIL_SENDER_ACCOUNT_SQL), 
        params = None
    )
    user, passwd = mail_sender_acc["user"].tolist()[0], mail_sender_acc["passwd"].tolist()[0]
    mail_sender = MailSender(
        smtp_server = smtp_server, 
        user = user, 
        passwd = passwd
    )


    cinema_city_dict = dict()
    df_get_all_cinema = db_conn.sql_to_dataframe(
        sql = text(GET_ALL_CINEMA_SQL), 
        params = None
    )
    for idx in df_get_all_cinema.index:
        if df_get_all_cinema["combine_name"][idx] == None:
            cinema_city_dict[df_get_all_cinema["cinema_name"][idx]] = df_get_all_cinema["city"][idx]
        else:
            cinema_city_dict[df_get_all_cinema["combine_name"][idx]] = df_get_all_cinema["city"][idx]

    cinema_mail_addr_dict = db_conn.sql_to_dataframe(
        sql = text(CINEMA_MAIL_ADDR_SQL), 
        params = None
    ).set_index("cinema_name").to_dict()["cinema_mail_addr"]
    city_mail_addr_dict = db_conn.sql_to_dataframe(
        sql = text(CITY_MAIL_ADDR_SQL), 
        params = None
    ).set_index("city").to_dict()["city_mail_addr"]
 
    cinema_list = cinema_mail_addr_dict.keys()
    city_list = city_mail_addr_dict.keys()
    cinema_excel_dict, city_excel_dict = dict(), defaultdict(list)
    for each_cinema in cinema_list:
        cinema_excel_name = "%s每日票房卖品数据(至%s)(含服务费).xlsx" % (each_cinema, date_name)
        if cinema_excel_name in os.listdir(path):
            cinema_excel_dict[each_cinema] = [cinema_excel_name]
    
    for each_cinema, each_city in cinema_city_dict.items():
        if each_city in city_list:
            city_excel_dict[each_city].append(cinema_excel_dict[each_cinema][0])

    for each_city in city_list:
        city_excel_name = "%s同城每日票房卖品数据(至%s)(含服务费).xlsx" % (each_city, date_name)
        if city_excel_name in os.listdir(path):
            city_excel_dict[each_city].append(city_excel_name)

    mail_sender.mail_login()
    mail_sender.send_mail(
        path = path, 
        entity_list = cinema_list, 
        files_dict = cinema_excel_dict, 
        receiver_mail_addr_dict = cinema_mail_addr_dict, 
        output = output, 
    )
    mail_sender.mail_quit()

    mail_sender.mail_login()
    mail_sender.send_mail(
        path = path, 
        entity_list = city_list, 
        files_dict = city_excel_dict, 
        receiver_mail_addr_dict = city_mail_addr_dict, 
        output = output,
    )
    mail_sender.mail_quit()
    output("所有报表已发送完毕\n")

    child_root.mainloop()

if __name__ == '__main__':
    # 注意自定义的日期，可以修改
    op_date = datetime.date.today() - datetime.timedelta(days = 1)
    # op_date = datetime.date(2024, 12, 30)
    serv_fee_dict, data_dict = dict(), dict()

    db_conn_from = DataBase(
        host = "192.168.16.114", 
        # host = "localhost", 
        user = "root", 
        passwd = "jy123456", 
        db = "sjzx"
    )

    db_conn_to = DataBase(
        host = "192.168.16.114", 
        # host = "localhost",
        user = "root", 
        passwd = "jy123456", 
        db = "jycinema_data"
    )
    ftp_conn = FTPData(
        host = "172.20.240.195", 
        user = "sjzx", 
        passwd = "jy123456@", 
        port = 21, 
        timeout = 30
    )

    root = tk.Tk()
    root.title("数据中心2.0")
    root.resizable(False, False)
    SetIcon(root, img1)
    CenterWindow(root, 400, 670)

    path = tk.StringVar()
    path.set(os.getcwd())
    file_path = tk.StringVar()

    tk.Label(root, text = "数据支持邮件发送程序v6.1", font=('微软雅黑',16)).grid(
        row = 0, column = 0, columnspan = 2, pady = 10, ipady = 15,
    )

    clock = tk.Label(root, font = ("times", 16, "bold"))
    clock.grid(
        row = 1, column = 0, columnspan = 2, pady = 10
    )
    ClockTick()

    tk.Button(root, text = "数据报表保存路径", command = SelectPath).grid(
        row = 2, column = 0, padx = 5, pady = 15
    )

    tk.Entry(root, textvariable = path).grid(
        row = 2, column = 1, ipadx = 10, pady = 15
    )

    tk.Button(root, text = "服务费文件选择", command = SelectFile).grid(
        row = 3, column = 0, padx = 20, ipadx = 10, pady = 15
    )

    tk.Button(root, text = "服务费处理", command = lambda: ProcessServiceFee(file_path.get(), serv_fee_dict)).grid(
        row = 3, column = 1, padx = 20, ipadx = 10, pady = 15
    )

    tk.Button(root, text = "下载日数据", command = lambda: download_daily_data(
        data_dict = data_dict, 
        path = path.get(), 
        op_date = op_date, 
        db_conn_from = db_conn_from, 
        db_conn_to = db_conn_to, 
        ftp_conn = ftp_conn, 
        child_window = ChildWindow(), 
        serv_fee_dict = serv_fee_dict
    )).grid(
        row = 4, column = 0, padx = 50, ipadx = 10, pady = 15
    )

    tk.Button(root, text = "上传日数据", command = lambda: upload_daily_data(
        data_dict = data_dict, 
        child_window = ChildWindow(), 
        field_dict = field_dict
    )).grid(
        row = 4, column = 1, padx = 50, ipadx = 10, pady = 15
    )

    tk.Button(root, text = "下载月数据", command = lambda: download_month_data(
        data_dict = data_dict, 
        path = path.get(), 
        op_date = op_date, 
        db_conn_from = db_conn_from, 
        db_conn_to = db_conn_to, 
        child_window = ChildWindow(), 
        serv_fee_dict = serv_fee_dict
    )).grid(
        row = 5, column = 0, padx = 50, ipadx = 10, pady = 15
    )

    tk.Button(root, text = "上传月数据", command = lambda: upload_month_data(
        data_dict = data_dict, 
        child_window = ChildWindow(), 
        field_dict = field_dict
    )).grid(
        row = 5, column = 1, padx = 50, ipadx = 10, pady = 15
    )

    tk.Button(root, text = "下载年数据", command = lambda: download_year_data(
        data_dict = data_dict, 
        path = path.get(), 
        op_date = op_date, 
        db_conn_from = db_conn_from, 
        db_conn_to = db_conn_to, 
        child_window = ChildWindow(), 
        serv_fee_dict = serv_fee_dict
    )).grid(
        row = 6, column = 0, padx = 50, ipadx = 10, pady = 15
    )

    tk.Button(root, text = "上传年数据", command = lambda: upload_year_data(
        data_dict = data_dict, 
        child_window = ChildWindow(), 
        field_dict = field_dict
    )).grid(
        row = 6, column = 1, padx = 50, ipadx = 10, pady = 15
    )

    tk.Label(root, text = "走势表运行线程数选择").grid(
        row = 7, column = 0, ipadx = 10, pady = 15
    )

    thread_num = tk.IntVar()
    thread_num_box = ttk.Combobox(root, width = 10, textvariable = thread_num, state = "readonly")
    thread_num_box['values'] = [4, 3, 2, 1]
    thread_num_box.grid(
        row = 7, column = 1, pady = 15
    )
    thread_num_box.current(0)
    thread_num_box.bind("<<ComboboxSelected>>", lambda event: thread_num.set(thread_num_box.get()))

    tk.Button(root, text = "一键生成影城/同城走势表", command = lambda: create_all_excel(
        path = path.get(), 
        db_conn = db_conn_to, 
        op_date = op_date, 
        child_window = ChildWindow(), 
        thread_num = thread_num.get()
    )).grid(
        row = 8, column = 0, columnspan = 2, padx = 50, ipadx = 10, pady = 15
    )

    tk.Button(root, text = "一键发送影城/同城邮箱", command = lambda: sendmail_all_excel(
        path = path.get(), 
        db_conn = db_conn_to, 
        op_date = op_date, 
        child_window = ChildWindow(), 
    )).grid(
        row = 9, column = 0, columnspan = 2, padx = 50, ipadx = 10, pady = 15
    )

    tk.Button(root, text = "退出程序", command = root.destroy).grid(
        row = 10, column = 0, columnspan = 2, padx = 20, ipadx = 60, pady = 15
    )

    root.mainloop()
