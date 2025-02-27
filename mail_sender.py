import os
import smtplib
from smtplib import SMTPException
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formataddr
import time, random


class MailSender:
    """
    初始化发送邮件的SMTP服务器信息

    Args:
        smtp_server (str): SMTP服务器地址
        user (str): 发送邮件的用户名
        passwd (str): 发送邮件的密码

    Returns:
        None

    Attributes:
        smtp_server (str): SMTP服务器地址
        user (str): 发送邮件的用户名
        passwd (str): 发送邮件的密码
        sender (str): 发送邮件的发送者地址，默认为用户名
        smtp (object): SMTP连接对象，初始化为None

    """
    def __init__(self, smtp_server, user, passwd):
        self.smtp_server = smtp_server
        self.user = user
        self.passwd = passwd
        self.sender = user
        self.smtp = None

    """
    登录邮箱并返回SMTP对象。

    Args:
        无参数。

    Returns:
        smtplib.SMTP_SSL: SMTP对象，用于发送邮件。

    """
    def mail_login(self):
        self.smtp = smtplib.SMTP_SSL(self.smtp_server, 465)
        self.smtp.connect(self.smtp_server, 465)
        self.smtp.ehlo()
        self.smtp.login(user = self.user, password = self.passwd)

    """
    发送邮件函数

    Args:
        entity_list (list): 实体列表，可以是影院列表或城市列表
        files_dict (dict): 字典类型，key为实体名称，value为文件列表，表示每个实体对应的所有文件
        child_window_func (function): 回调函数，用于在子线程中执行，返回值为子窗口的根窗口对象和输出函数
        path (str): 当前工作目录
        receiver_mail_addr (dict): 字典类型，key为实体名称，value为邮件接收者地址

    Returns:
        None

    """
    def send_mail(self, path, entity_list, files_dict, receiver_mail_addr_dict, output):
        # entity_list: [cinema1, cinema2, ..., cinema_n] or [city1, city2, ..., city_n]
        # files_dict :  dict(key: [val1, val2, ..., valn]) -> cinema_name:[cinema_xlsx] or city:[cinema1_xlsx, cinema2_xlsx, ..., cinema_n_xlsx, city_xlsx]
        # receiver_mail_addr_dict: dict(entity1:mail_addr1, entity2:mail_addr2, ..., entity_n:mail_addr_n)
        os.chdir(path)
        suc_cnt, fail_cnt = 0, 0
        for each_entity in entity_list:
            if each_entity in files_dict.keys():
                msg = MIMEMultipart()
                # 同城文件放最尾
                topic = files_dict[each_entity][-1].rstrip(".xlsx")
                describes = """
                （1）票房数据含服务费，票房取自实时报表，服务费取自vista系统。\n
                （2）卖品数据取自infowork，按交易时间统计，含第三方预售部分，且含卖品销售折扣（含抖音套餐）。
                """
                each_receiver = receiver_mail_addr_dict[each_entity]
                msg["Subject"], text = topic, MIMEText(topic + "\n" + describes)
                msg["From"], msg["To"] = formataddr(["信息数据分析研究中心", self.sender]), each_receiver
                msg.attach(text)
                for each_file in files_dict[each_entity]:
                    att = MIMEApplication(open(each_file, "rb").read())
                    att.add_header("Content-Disposition", "attachment", filename=("GBK", "", each_file))
                    msg.attach(att)
                try:
                    if "," in each_receiver:
                        self.smtp.sendmail(self.sender, each_receiver.split(","), msg.as_string())
                    else:
                        self.smtp.sendmail(self.sender, each_receiver, msg.as_string())
                    output("%s 发送成功!\n" % each_entity)
                    suc_cnt += 1
                except SMTPException as e:
                    fail_cnt += 1
                    output("%s 发送失败！\n" % each_entity)

                time.sleep(random.randint(1, 2))

        output("发送成功邮件数: %s, 发送失败邮件数: %s\n" % (suc_cnt, fail_cnt))

    """
    关闭与SMTP服务器的连接

    Args:
        无

    Returns:
        无

    """
    def mail_quit(self):
        self.smtp.quit()