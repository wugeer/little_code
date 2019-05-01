# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: linezone-peacebird-wh-1
# Filename: p_mod_table_day_data_duplication
# Author: zsm
# Date: 2018/10/29 20:32
# ----------------------------------------------------------------------------

import os
from data_monitor import DataMonitor
from data_monitor.MonitorConfig import DataDupConfig
from utils.config import PBConfig
from utils.tools import TaskThreadPoolExecutors


class DataDup(DataMonitor):

    def __init__(self, file_name):
        # 继承DataMonitor的__init__方法
        DataMonitor.__init__(self, file_name)
        self.params_dict['tmp'] = DataDupConfig.TMP
        self.params_dict['target_table'] = DataDupConfig.TARGET_TABLE
        self.params_dict['schema'] = PBConfig.EDW_SCHEMA

    def data_dup(self):
        statis_sql = DataDupConfig.DATA_DUP_SQL
        insert_sql = DataDupConfig.INSERT_SQL
        # schema中包含的所有表，目前schema默认为utils中设置的edw名
        table_name_list = self.show_table_names()
        # 目前只检查模型表是否有重复数据,获得所有模型表名
        table_name_list = [t for t in table_name_list
                           if t.find('mod') != -1 and t != self.params_dict['target_table']]
        statis_sdf = self.get_statis_sdf(statis_sql, table_name_list)
        self.if_exists_dup(statis_sdf)
        self.execute_sql(insert_sql)
        self.drop_temp_table(self.params_dict['tmp'])

    def if_exists_dup(self, statis_sdf):
        data_dup_sdf = statis_sdf.filter(statis_sdf.is_dup == 'Y')
        if not data_dup_sdf.rdd.isEmpty():
            dup_table_list = data_dup_sdf.select('table_name').toPandas()['table_name'].tolist()
            dup_table_str = '\n'.join(dup_table_list)
            # 邮件主题
            email_subject = DataDupConfig.SUBJECT \
                .format(Monitor_class=self.__class__.__name__, project=DataDupConfig.PROJECT)
            # 邮件内容
            email_content = DataDupConfig.DATA_DUP_EMAIL_CONT \
                .format(p_input_date=self.params_dict['p_input_date'], dup_table_str=dup_table_str)
            DataDup.send_email(email_subject, email_content)
        else:
            print('All tables you want to check are normal')


# 命令：spark2-submit p_mod_table_day_data_duplication.py 2018-08-27
if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    data_dup = DataDup(file_name)
    data_dup.data_dup()