# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_rst_skc_classify_day_analysis
   Description :
   Author :       yangming
   date：          2018/9/12
-------------------------------------------------
   Change Activity:
                   2018/9/12:
-------------------------------------------------
"""
import os

from utils.tools import SparkInit

if __name__ == '__main__':
    # app_name
    file_name = os.path.basename(__file__)

    # 初始化spark环境
    foo = SparkInit(file_name)

    sql = """
        insert overwrite table {p_rst_schema}.rst_skc_classify_day_analysis
        partition(day_date)
        select a.year_id as year
            , a.quarter_id as quarter
            , a.prod_class
            , a.stage_lifetime
            , a.total_sales_qty
            , a.sales_rate
            , current_timestamp as etl_time
            , a.day_date
        from {p_dm_schema}.dm_skc_classify_day_analysis a 
        where a.day_date = '{p_input_date}'
    """

    foo.execute_sql(sql)