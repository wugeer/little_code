# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: linezone_peacebird_wh_1
# Filename: p_replenish_model_data_check
# Author: zsm
# Date: 2018/11/14 20:48
# ----------------------------------------------------------------------------
import os
import sys
from utils.tools import SparkInit


# app_name
file_name = os.path.basename(__file__)

# 初始化spark环境
foo = SparkInit(file_name)


def ai_data_check():
    # sql部分
    sql = """          
        select etl_time, status
        from {p_edw_schema}.model_flag
        where 
            day_date = '{p_input_date_add_one_day}'
            and category = 'replenish_model'
    """
    df = foo.return_df(sql)
    if not df.rdd.isEmpty():
        info = df.toPandas().sort_values(by="etl_time", ascending=False)['status'].values
        status = info[0]
        if status == 1:
            print('Flag is True with status code {}'.format(status))
            return True
        else:
            print('flag is False with status code {}'.format(status))
            sys.exit(1)
    else:
        print('flag is None')
        sys.exit(1)


if __name__ == '__main__':
    ai_data_check()
