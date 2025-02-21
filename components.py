import time
import os
import tkinter as tk
import pandas as pd
from logo2 import img2
import base64


__all__ = ["CenterWindow", "SetIcon", "ChildWindow"]



# 窗口居中
def CenterWindow(root, width, height):
    screenwidth = root.winfo_screenwidth()
    screenheight = root.winfo_screenheight()
    size = "%dx%d+%d+%d" % (width, height, (screenwidth - width)/2, (screenheight - height)/2)
    root.geometry(size)

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
    # 这里打包时要引入img2的图标
    SetIcon(child_root, img2)
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

