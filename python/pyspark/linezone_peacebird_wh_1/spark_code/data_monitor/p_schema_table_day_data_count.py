# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: linezone-peacebird-wh-1
# Filename: p_schema_table_day_data_count
# Author: zsm
# Date: 2018/10/29 10:47
# ----------------------------------------------------------------------------

import sys
import os
from data_monitor import DataMonitor
from data_monitor.MonitorConfig import DataCountConfig


class DataCount(DataMonitor):

    def __init__(self, file_name):
        # 继承DataMonitor的__init__方法
        DataMonitor.__init__(self, file_name)
        self.params_dict['tmp'] = DataCountConfig.TMP
        self.params_dict['target_table'] = DataCountConfig.TARGET_TABLE
        self.params_dict['schema'] = sys.argv[2]
        # 目的是解决：传入的schema为如dms，而dms下所有table_name的前缀都是dm......
        for table_prefix in DataCountConfig.TABLE_PREFIX:
            if self.params_dict['schema'].find(table_prefix) != -1:
                self.params_dict['table_prefix'] = table_prefix

    def data_count(self):
        statis_sql = DataCountConfig.DATA_COUNT_SQL
        insert_sql = DataCountConfig.INSERT_SQL
        table_name_list = self.show_table_names()
        statis_sdf = self.get_statis_sdf(statis_sql, table_name_list)
        self.if_exists_empty(statis_sdf)
        self.execute_sql(insert_sql)
        self.drop_temp_table(self.params_dict['tmp'])

    def if_exists_empty(self, statis_sdf):
        data_empty_sdf = statis_sdf.filter(statis_sdf.data_count == 0)
        if not data_empty_sdf.rdd.isEmpty():
            empty_table_list = data_empty_sdf.select('table_name').toPandas()['table_name'].tolist()
            empty_table_str = '\n'.join(empty_table_list)
            # 邮件主题
            email_subject = DataCountConfig.SUBJECT \
                .format(Monitor_class=self.__class__.__name__, project=DataCountConfig.PROJECT)
            # 邮件内容
            email_content = DataCountConfig.DATA_COUNT_EMAIL_CONT \
                .format(p_input_date=self.params_dict['p_input_date'], empty_table_str=empty_table_str)
            DataCount.send_email(email_subject, email_content)
        else:
            print('All tables you want to check are normal')


# 命令：spark2-submit p_schema_table_day_data_count.py 2018-08-27 schema名
if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    data_count = DataCount(file_name)
    data_count.data_count()