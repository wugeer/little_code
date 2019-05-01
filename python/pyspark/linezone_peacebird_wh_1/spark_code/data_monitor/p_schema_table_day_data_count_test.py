# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: linezone-peacebird-wh-1
# Filename: p_schema_table_day_data_count
# Author: zsm
# Date: 2018/10/29 10:47
# ----------------------------------------------------------------------------

import sys

import os
import pandas as pd
from data_monitor import DataMonitor
from utils.tools import TaskThreadPoolExecutors
from utils.config import PBConfig

class DataCount(DataMonitor):

    def __init__(self, file_name):
        # 继承DataMonitor的__init__方法
        DataMonitor.__init__(self, file_name)
        self.params_dict['data_count_tmp_table_name'] = 'data_count_tmp'
        # 目的是解决：传入的schema为如dms，而dms下所有table_name的前缀都是dm......
        for table_prefix in PBConfig.TABLE_PREFIX:
            if self.params_dict['schema'].find(table_prefix) != -1:
                self.params_dict['table_prefix'] = table_prefix

    def data_count(self):
        data_count_sql = """
            select '{schema}.{table_name}' as table_name
              , count(1) as data_count
              , '{p_input_date}' as day_date 
            from {schema}.{table_name} 
            where etl_time > '{p_input_date}'
        """
        # 要检查数据量的schema中包含的所有表
        table_name_list = self.show_table_name()
        # 检查某schema中所有表数据量的sql语句列表
        data_count_sql_list = self.format_sql_list(data_count_sql, table_name_list)
        # 多线程执行检查各个表数据量的sql
        data_count_multi = TaskThreadPoolExecutors(self.return_df, 20, data_count_sql_list)
        data_count_sdf = self.got_multi_result(data_count_multi, self.params_dict['data_count_tmp_table_name'])

        # 插入data_count表中对应schema的各表的数据量
        insert_sql = """
            insert overwrite table {schema}.{table_prefix}_table_day_data_count
            partition(day_date)
            select 
                {data_count_tmp_table_name}.table_name
                , {data_count_tmp_table_name}.data_count
                , current_timestamp as etl_time
                , '{p_input_date}' as day_date
            from {data_count_tmp_table_name}
            """
        self.execute_sql(insert_sql)
        self.drop_temp_table(self.params_dict['data_count_tmp_table_name'])
        self.if_exist_empty(data_count_sdf)

    def if_exist_empty(self, data_count_sdf):

        # data_count_sdf.unpersist()
        # # self.spark.catalog.refreshTable('{schema}.{table_prefix}_table_day_data_count'.format(**self.params_dict))

        # # print(self.spark.table(self.params_dict['data_count_tmp_table_name']).show())

        print(data_count_sdf.show())
        data_empty_sdf = data_count_sdf.filter(data_count_sdf.data_count == 0)

        if not data_empty_sdf.rdd.isEmpty():
            print('dudu')
            empty_table_list = data_empty_sdf.select('table_name').toPandas()['table_name'].tolist()
            empty_table_str = "&".join(empty_table_list)
            print(empty_table_str)
        else:
            return



# 命令：spark2-submit p_schema_table_day_data_count.py 2018-08-27 schema名
if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    data_count = DataCount(file_name)
    data_count.data_count()