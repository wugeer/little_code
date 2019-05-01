# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_store_day_sales_analysis
# Author: zsm
# Date: 2018/9/10 18:00
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql = '''
        insert overwrite table {p_dm_schema}.dm_store_day_sales_analysis partition(day_date)
        select 
            store_code
            , sum(last_fourteen_days_sales_qty) as last_fourteen_days_sales_qty
            , sum(last_week_sales_qty) as last_week_sales_qty
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from dm.dm_skc_store_day_sales
        where 
            day_date = '{p_input_date}'
        group by
            store_code
    '''
    spark.execute_sql(sql)