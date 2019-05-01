# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: linezone-peacebird-wh-1
# Filename: __init__
# Author: zsm
# Date: 2018/10/29 21:26
# ----------------------------------------------------------------------------
import sys
import copy
from email.mime.text import MIMEText
from email.header import Header
from email import encoders
from email.utils import parseaddr, formataddr
import smtplib
from utils.tools import SparkInit
from utils.tools import TaskThreadPoolExecutors
from data_monitor.MonitorConfig import MonitorConfig


class DataMonitor(SparkInit):

    def __init__(self, file_name):
        # 继承SparkInit的__init__方法
        SparkInit.__init__(self, file_name)

    def show_table_names(self):
        sql = MonitorConfig.SHOW_TABLE_NAMES_SQL \
            .format(schema=self.params_dict['schema'])
        spark_df = self.return_df(sql)
        table_name_list = spark_df.toPandas()['tableName'].tolist()
        return table_name_list

    def show_table_columns(self, table_name):
        sql = MonitorConfig.SHOW_TABLE_COLUMNS_SQL \
            .format(schema=self.params_dict['schema'], table_name=table_name)
        spark_df = self.return_df(sql)
        table_columns_list = spark_df.toPandas()['col_name'].tolist()
        return table_columns_list

    def format_sql_list(self, sql_name, all_table_info_list):
        sql_list = [sql_name.format(**table_info, **self.params_dict)
                    for table_info in all_table_info_list]
        return sql_list

    @staticmethod
    def got_multi_result(multi_name, tmp):
        # 获取多线程返回的结果
        results = None
        for result in multi_name.result:
            if results is None:
                results = result
            else:
                results = results.unionAll(result)
        results.createOrReplaceTempView(tmp)
        return results

    def get_statis_sdf(self, statis_sql, table_name_list):
        # 获取每个表的所有字段,将etl_time字段移除;
        # 将模型表中标志日期的字段(cla_week_date/dec_day_date等)识别到where条件中
        # all_table_columns_list的数据结构为
        # [{'columns':[], 'where_condi':str1}, {[],[]}， {[],[]}......]
        all_table_info_list = []
        for table_name in table_name_list:
            table_info_dict = {}
            table_columns_list_init = self.show_table_columns(table_name)
            table_columns_list = copy.deepcopy(table_columns_list_init)
            where_condi = "where 1=1"
            date = 'day_date'
            for column in table_columns_list_init:
                if column in MonitorConfig.ETL_TIME:
                    table_columns_list.remove(column)
                if column in MonitorConfig.DAY_DATE_LIST:
                    date = column
                    where_condi = "where {date} = '{p_input_date}'" \
                        .format(date=date, p_input_date=self.params_dict['p_input_date'])
                    table_columns_list.remove(column)
            if table_name in MonitorConfig.WEEK_MON_TABLE_LIST:
                where_condi = "where {date} = '{p_input_date}'" \
                    .format(date=date, p_input_date=self.params_dict['p_input_date_mon'])
            if table_name in MonitorConfig.ADD_ONE_DAY_TABLE_LIST:
                where_condi = "where {date} = '{p_input_date}'" \
                    .format(date=date, p_input_date=self.params_dict['p_input_date_add_one_day'])
            if table_name in MonitorConfig.ADD_ONE_DAY_MON_TABLE_LIST:
                where_condi = "where {date} = '{p_input_date}'" \
                    .format(date=date, p_input_date=self.params_dict['p_input_date'])
            if table_name in MonitorConfig.SUB_SIX_DAY_MON_TABLE_LIST:
                where_condi = "where {date} = '{p_input_date}'" \
                    .format(date=date, p_input_date=self.params_dict['p_input_date_sub_six_mon'])
            table_info_dict['table_name'] = table_name
            table_info_dict['group_by_columns'] = "`,`".join(table_columns_list)
            table_info_dict['where_condi'] = where_condi
            all_table_info_list.append(table_info_dict)
        statis_sql_list = self.format_sql_list(statis_sql, all_table_info_list)
        multi = TaskThreadPoolExecutors(self.return_df, 20, statis_sql_list)
        statis_sdf = DataMonitor.got_multi_result(multi, self.params_dict['tmp'])
        return statis_sdf

    @staticmethod
    def send_email(subject, content):
        # 第三方 SMTP 服务
        mail_host = MonitorConfig.MAIL_HOST
        mail_port = MonitorConfig.MAIL_PORT
        mail_user = MonitorConfig.MAIL_USER
        mail_pass = MonitorConfig.MAIL_PASSWD
        sender = MonitorConfig.SENDER
        receivers = MonitorConfig.RECEIVERS
        sender_name = MonitorConfig.SENDER_NAME
        message = MIMEText(content, 'plain', 'utf-8')
        message['From'] = '{sender_name} <{sender}>' \
            .format(sender_name=sender_name, sender=sender)
        message['To'] = ",".join(receivers)
        message['Subject'] = Header(subject, 'utf-8').encode()
        try:
            smtp = smtplib.SMTP()
            smtp.connect(mail_host, mail_port)
            smtp.login(mail_user, mail_pass)
            smtp.sendmail(sender, receivers, message.as_string())
            smtp.quit()
            print("邮件发送成功")
        except smtplib.SMTPException:
            print("Error: 发送邮件失败")
