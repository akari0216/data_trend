# 纯SQL语句，传入字段参数后再调用sqlalchemy的text进行参数化查询



JY_INFO_SQL = """
SELECT cinema_code, cinema_name, concat(hall_count, '厅', seats_count, '座') as cinema_info 
FROM jycinema_info 
WHERE cinema_code = :cinema_code
"""

JY_DATA_YEAR_SQL = """
SELECT {}, `year` 
FROM jycinema_data_year 
WHERE cinema_code = :cinema_code and year >= :begin_year
"""

JY_DATA_MONTH_SQL = """
SELECT {}, `month`
FROM jycinema_data_month
WHERE cinema_code = :cinema_code and year = :year and month <= :end_month
"""

JY_DATA_DAILY_SQL = """
SELECT {}, op_date
FROM jycinema_data_daily
WHERE cinema_code = :cinema_code and year = :year and month = :month and op_date <= :op_date
"""


COMPETE_INFO_SQL = """
SELECT compete_cinema_code, compete_cinema_name, concat(drive_dist, 'km,', hall_count, '厅,', seats_count,'座') as compete_info, compete_level, total_weight 
FROM compete_cinema_info 
WHERE jycinema_code = :cinema_code
ORDER BY drive_dist asc
"""

COMPETE_DATA_YEAR_SQL = """
SELECT {}, `year`
FROM compete_cinema_data_year 
WHERE compete_cinema_code = :compete_cinema_code and year >= :begin_year
"""

COMPETE_DATA_MONTH_SQL = """
SELECT {}, `month`
FROM compete_cinema_data_month 
WHERE compete_cinema_code = :compete_cinema_code and year = :year and month <= :end_month
"""

COMPETE_DATA_DAILY_SQL = """
SELECT {}, op_date 
FROM compete_cinema_data_daily 
WHERE compete_cinema_code = :compete_cinema_code and year = :year and month = :month and op_date <= :op_date
"""

JY_CONSESSION_YEAR_SQL = """
SELECT {}, `year` 
FROM jycinema_consession_year 
WHERE cinema_code = :cinema_code and year >= :begin_year
"""

JY_CONSESSION_MONTH_SQL = """
SELECT {}, `month` 
FROM jycinema_consession_month 
WHERE cinema_code = :cinema_code and year = :year and month <= :end_month
"""

JY_CONSESSION_DAILY_SQL = """
SELECT {}, op_date 
FROM jycinema_consession_daily 
WHERE cinema_code = :cinema_code and year = :year and month = :month and op_date <= :op_date
"""

JY_CINEMA_CITY_SQL = """
SELECT cinema_code, city, city_cal_status 
FROM jycinema_info 
WHERE cinema_code = :cinema_code
"""

JY_MARKET_RATIO_REVISE_SQL = """
SELECT market_ratio_revise 
FROM cinema_market_ratio_revise 
WHERE cinema_code = :cinema_code and year = :year and month <= :month
"""

JY_CITY_MARKET_RATIO_REVISE_SQL = """
SELECT market_ratio_revise 
FROM city_market_ratio_revise 
WHERE city = :city and year = :year and month <= :month
"""

JY_MARKET_RATIO_TARGET_SQL = """
SELECT market_ratio_target 
FROM cinema_market_ratio_target 
WHERE cinema_code = :cinema_code and year = :year and month <= :month
"""

JY_CITY_MARKET_RATIO_TARGET_SQL = """
SELECT market_ratio_target 
FROM city_market_ratio_target 
WHERE city = :city and year = :year and month <= :month
"""

JY_BO_TARGET_SQL = """
SELECT bo_target_accum 
FROM cinema_bo_target 
WHERE cinema_code = :cinema_code and year = :year and month <= :month
"""

JY_CITY_BO_TARGET_SQL = """
SELECT bo_target_accum 
FROM city_bo_target 
WHERE city = :city and year = :year and month <= :month
"""

JY_SERV_FEE_YEAR_SQL = """
SELECT {}, `year` 
FROM jycinema_serv_fee_year 
WHERE cinema_code = :cinema_code and year >= :begin_year
"""

JY_SERV_FEE_MONTH_SQL = """
SELECT {}, `month` 
FROM jycinema_serv_fee_month 
WHERE cinema_code = :cinema_code and year = :year and month <= :end_month
"""

JY_SERV_FEE_DAILY_SQL = """
SELECT {}, op_date 
FROM jycinema_serv_fee_daily 
WHERE cinema_code = :cinema_code and year = :year and month = :month and op_date <= :op_date
"""

COMPETE_SERV_FEE_YEAR_SQL = """
SELECT {}, `year` 
FROM compete_cinema_serv_fee_year 
WHERE compete_cinema_code = :compete_cinema_code and year >= :begin_year
"""

COMPETE_SERV_FEE_MONTH_SQL = """
SELECT {}, `month` 
FROM compete_cinema_serv_fee_month 
WHERE compete_cinema_code = :compete_cinema_code and year = :year and month <= :end_month
"""

COMPETE_SERV_FEE_DAILY_SQL = """
SELECT {}, op_date 
FROM compete_cinema_serv_fee_daily 
WHERE compete_cinema_code = :compete_cinema_code and year = :year and month = :month and op_date <= :op_date
"""

CINEMA_CODE_FROM_COMBINE_CODE_SQL = """
SELECT cinema_code
FROM jycinema_info
WHERE combine_code = :combine_code
"""

COMBINE_NAME_SQL = """
SELECT combine_name
FROM jycinema_info
WHERE combine_code = :combine_code
"""

CITY_LIST_SQL = """
SELECT distinct(city)
FROM jycinema_info
WHERE op_status = 1
"""

CINEMA_LIST_EXCEPT_COMBINE_SQL = """
SELECT cinema_code, cinema_name, city
FROM jycinema_info 
WHERE city = :city 
AND combine_code is null
AND op_status = 1
AND city_cal_status = 1
"""

COMBINE_CINEMA_LIST_SQL = """
SELECT distinct(combine_code), combine_name, city 
FROM jycinema_info 
WHERE city = :city 
AND combine_code is not null 
AND op_status = 1 
AND city_cal_status = 1
"""

GET_COMPETE_DATA_SQL = """
SELECT cinema_code, cinema_name, movie_name, bo, people, occupancy 
FROM topcdb_compete_cinema_daily 
WHERE op_date = :date
"""

GET_COMPETE_SERV_FEE_SQL = """
SELECT cinema_code, cinema_name, fee 
FROM topcdb_compete_cinema_daily 
WHERE op_date = :date
"""

MAIL_SENDER_ACCOUNT_SQL = """
SELECT user, passwd 
FROM mail_sender_account
"""

CINEMA_MAIL_ADDR_SQL = """
SELECT cinema_name, cinema_mail_addr 
FROM cinema_mail_addr 
WHERE cinema_mail_addr is NOT NULL
"""

CITY_MAIL_ADDR_SQL = """
SELECT city, city_mail_addr 
FROM city_mail_addr
"""

GET_ALL_CINEMA_SQL = """
SELECT combine_name, cinema_name, city 
FROM jycinema_info 
WHERE op_status = 1
"""

