from formatter_styles import *
import numpy as np


def xls_formatter_cinema(wb, df, sheet_name, weight):
    wb.guess_types = True
    ws = wb.create_sheet(sheet_name)
    df = df.T
    cols_list = df.iloc[0].tolist()
    
    df = df.iloc[1:]
    m, n = df.shape
    k = m - len(weight)
    data_list = np.array(df).tolist()
    #市占部分
    market_percent_list = []
    if sheet_name == "票房数据":
        market_percent_list = data_list[-k:]
        data_list = data_list[:-k]
        m = m - k
    #写入首行
    ws.append(cols_list)
    ws.row_dimensions[1].height = 20
    for i in range(1, n + 1):
        cell = ws.cell(row = 1, column = i)
        cell.alignment = alignment1
        cell.font = font1
        cell.fill = fill1
        cell.border = border1
        if i == n:
            cell.border = border2
        if sheet_name in ["票房数据", "人次数据", "上座率数据", "票价数据"] and i == 4:
            cell.border = border2
        if sheet_name == "卖品数据" and i == 1:
            cell.border = border2
    
    #写入数据
    for each_data in data_list:
        ws.append(each_data)
    #按字段区分两类进行渲染
    if sheet_name in ["票房数据", "人次数据", "上座率数据", "票价数据"]:
        for i in range(2, m + 2):
            ws.row_dimensions[i].height = 15
            for j in range(1, n + 1):
                cell = ws.cell(row = i, column = j)
                cell.alignment = alignment1
                if j == 4 or j == n:
                    cell.border = border3
                if i == 2:
                    cell.font = font2
                elif i > 2:
                    cell.font = font3
                if j <= 4:
                    cell.fill = fill2
                else:
                    cell.number_format = "0.00"
        #添加底部边框
        for j in range(1, n + 1):
            cell = ws.cell(row = m + 1, column = j)
            cell.border = border4
            if j == 4 or j == n:
                cell.border = border5
                
        # 对票房数据的市占部分处理
        if sheet_name == "票房数据" and k > 0:
            for i in range(0, k):
                ws.append(market_percent_list[i])
                ws.row_dimensions[m + 2 + i].height = 15
                for j in range(1, n + 1):
                    cell = ws.cell(row = m + 2 + i, column = j)
                    cell.alignment = alignment1
                    cell.font = font4
                    if j > 4:
                        if i == k - 3:
                            cell.number_format = "0.00" 
                        else:
                            cell.number_format = "0.00%"
                    if j == n:
                        cell.border = border3
            for j in range(1, n + 1):
                cell = ws.cell(row = m + 1 + k, column = j)
                cell.border = border4
                if j == n:
                    cell.border = border5
        
        #第2-4列调整列宽
        ws.column_dimensions[get_column_letter(2)].width = 22
        ws.column_dimensions[get_column_letter(3)].width = 18
        ws.column_dimensions[get_column_letter(4)].width = 10
        #冻结窗格
        ws.freeze_panes = ws["E2"]
        
    elif sheet_name == "卖品数据":
        for i in range(2, m + 2):
            ws.row_dimensions[i].height = 15
            for j in range(1, n + 1):
                cell = ws.cell(row = i, column = j)
                cell.alignment = alignment1
                cell.font = font3
                cell.number_format = "0.00"
                if j == 1:
                    cell.fill = fill2
                    cell.border = border3
                if j == n:
                    cell.border = border3
                    
        for j in range(1, n + 1):
            cell = ws.cell(row = m + 1, column = j)
            cell.border = border4
            if j == 1 or j == n:
                cell.border = border5
                
        ws.column_dimensions[get_column_letter(1)].width = 15
        ws.freeze_panes = ws["B2"]
                
    return wb


def xls_formatter_city(wb, df, sheet_name, weight):
    wb.guess_types = True
    ws = wb.create_sheet(sheet_name)
    df = df.T
    cols_list = df.iloc[0].tolist()
    df = df.iloc[1:]
    data_list = np.array(df).tolist()
    #附加部分
    attach_list = []
    if sheet_name == "票房数据":
        attach_list = data_list[len(weight):]
        data_list = data_list[:len(weight)]
    elif sheet_name in [ "人次数据", "上座率数据", "票价数据"]:
        attach_list = data_list[-1]
        data_list = data_list[:-1]
    elif sheet_name == "卖品数据":
        attach_list = data_list[-4:]
        data_list = data_list[:-4]

    m, n, k = len(data_list), len(cols_list), len(attach_list)
    cinema_index_list = []
    for i, x in enumerate(weight):
        if x == 0.175:
            cinema_index_list.append(i)

    #写入首行
    ws.append(cols_list)
    ws.row_dimensions[1].height = 20
    for i in range(1, n + 1):
        cell = ws.cell(row = 1,column = i)
        cell.alignment = alignment1
        cell.font = font1
        cell.fill = fill1
        cell.border = border1
        if i == n:
            cell.border = border2
        if sheet_name in ["票房数据", "人次数据", "上座率数据", "票价数据"] and i == 4:
            cell.border = border2
        if sheet_name == "卖品数据" and i == 3:
            cell.border = border2
            
    #写入数据
    for each_data in data_list:
        ws.append(each_data)
    #按字段区分两类进行渲染
    if sheet_name in ["票房数据", "人次数据", "上座率数据", "票价数据"]:
        for i in range(2, m + 2):
            ws.row_dimensions[i].height = 15
            for j in range(1, n + 1):
                cell = ws.cell(row = i, column = j)
                cell.alignment = alignment1
                if j == 4 or j == n:
                    cell.border = border3
                #自营影城加红
                if i - 2 in cinema_index_list:
                    cell.font = font2
                    if i - 2 != 0:
                        cell.border = border10
                        if j == 4 or j == n:
                            cell.border = border11
                elif i - 2 not in cinema_index_list:
                    cell.font = font3
                if j <= 4:
                    cell.fill = fill2
                else:
                    cell.number_format = "0.00"
                  
        #添加底部边框
        for j in range(1, n + 1):
            cell = ws.cell(row = m + 1, column = j)
            cell.border = border4
            if j == 4 or j == n:
                cell.border = border5
                
        #附加的汇总部分
        if sheet_name == "票房数据":
            for i in range(0, k):
                ws.append(attach_list[i])
                ws.row_dimensions[m + 2 + i].height = 15
                for j in range(1, n + 1):
                    cell = ws.cell(row = m + 2 + i, column = j)
                    cell.alignment = alignment1
                    cell.font = font4
                    if j == n:
                        cell.border = border3
                    if i == 0 or i == k - 3:
                        cell.number_format = "0.00"
                    elif i != 0:
                        cell.number_format = "0.00%"
            #添加底部边框
            for j in range(1, n + 1):
                cell = ws.cell(row = m + 1 + k, column = j)
                cell.border = border4
                if j == n:
                    cell.border = border5
                
        else:
            ws.append(attach_list)
            ws.row_dimensions[m + 2].height = 15
            for j in range(1, n + 1):
                cell = ws.cell(row = m + 2, column = j)
                cell.alignment = alignment1
                cell.font = font4
                cell.border = border4
                cell.number_format = "0.00"
                if j == n:
                    cell.border = border5
        
        #调整列宽，冻结窗格
        ws.column_dimensions[get_column_letter(1)].width = 10
        ws.column_dimensions[get_column_letter(2)].width = 20
        ws.column_dimensions[get_column_letter(3)].width = 18
        ws.column_dimensions[get_column_letter(4)].width = 10            
        ws.freeze_panes = ws["E2"]
                    
    elif sheet_name == "卖品数据":
        for i in range(2,m + 2):
            ws.row_dimensions[i].height = 15
            for j in range(1, n + 1):
                cell = ws.cell(row = i, column = j)
                cell.alignment = alignment1
                cell.font = font3
                if j <= 3:
                    cell.fill = fill2
                if (i - 1) % 4 == 0 and i != m + 1:
                    cell.border = border8
                if i == m + 1:
                    cell.border = border4
                if j == 3 or j == n:
                    cell.border = border3 
                if (j == 3 or j == n) and ((i - 1) % 4 == 0 and i != m + 1):
                    cell.border = border9

        #添加底部边框
        for j in range(1, n + 1):
            cell =ws.cell(row = m + 1, column = j)
            cell.border = border4
            if j == 3 or j == n:
                cell.border = border5
                
        #附加的汇总部分
        for i in range(0, k):
            ws.append(attach_list[i])
            ws.row_dimensions[m + 2 + i].height = 15
            for j in range(1, n + 1):
                cell = ws.cell(row = m + 2 + i, column = j)
                cell.alignment = alignment1
                cell.font = font4
                cell.number_format = "0.00"
                if j == n:
                    cell.border = border3
        
        for j in range(1, n + 1):
            cell = ws.cell(row = m + 1 + k, column = j)
            cell.border = border4
            if j == n:
                cell.border = border5

        #调整列宽，冻结窗格
        ws.column_dimensions[get_column_letter(1)].width = 10
        ws.column_dimensions[get_column_letter(2)].width = 20
        ws.column_dimensions[get_column_letter(3)].width = 15                
        ws.freeze_panes = ws["D2"]
    
    return wb