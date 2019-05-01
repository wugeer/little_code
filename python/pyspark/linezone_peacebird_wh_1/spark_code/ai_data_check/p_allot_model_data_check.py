# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: linezone_peacebird_wh_1
# Filename: p_allot_model_data_check
# Author: yangzhitao
# Date: 2019/01/11 16:30
# ----------------------------------------------------------------------------
import os
import sys
from datetime import datetime
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
            and category = 'allot_model'
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


def ai_check_weekday(weekday):
    """
    :param weekday: int, 1: Monday, ... 7: Sunday
    :return: bool
    """
    assert isinstance(weekday, int)
    param_list = sys.argv
    p_input_date = datetime.strptime(param_list[1], '%Y-%m-%d')
    return p_input_date.isocalendar()[2] == weekday


if __name__ == '__main__':
    if ai_check_weekday(1):
        # run at Monday end
        ai_data_check()
