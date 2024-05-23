import time
import os
import tkinter as tk
from tkinter.filedialog import askdirectory


__all__ = ["ClockTick", "CenterWindow", "SelectPath", "SetIcon", "ChildWindow"]


# 时间显示
def ClockTick():
    global time1
    time2 = time.strftime("%Y-%m-%d %H:%M:%S")
    if time2 != time1:
        time1 = time2
        clock.config(text = time2)
    clock.after(200, tick)

# 窗口居中
def CenterWindow(root, width, height):
    screenwidth = root.winfo_screenwidth()
    screenheight = root.winfo_screenheight()
    size = "%dx%d+%d+%d" % (width, height, (screenwidth - width)/2, (screenheight - height)/2)
    root.geometry(size)

# 路径选择 
def SelectPath():
    path_ = askdirectory()
    path.set(path_)

# 引入图标文件
def SetIcon(root, img):
    tmp = open("tmp.ico", "wb+")
    tmp.write(base64.b64decode(img))
    tmp.close()
    root.iconbitmap("tmp.ico")
    os.remove("tmp.ico")

# 子窗口控件
def ChildWindow():
    # 生成子窗口
    child_root = tk.Toplevel()
    child_root.title("运行记录")
    # 这里打包时要引入logo1.img1的图标
    # SetIcon(child_root, img1)
    child_root.geometry("330x160+300+300")
    S = tk.Scrollbar(child_root)
    T = tk.Text(child_root, height = 8, width = 240)
    S.pack(side = tk.RIGHT, fill = tk.Y)
    T.pack(side = tk.LEFT, fill = tk.Y)
    #文本框和滚动条互相绑定
    S.config(command = T.yview)
    T.config(yscrollcommand = S.set)
    def output(quote):
        T.insert(tk.END, quote)
        T.see(tk.END)
        T.update()
    return child_root, output