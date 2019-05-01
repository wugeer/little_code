# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_org_day_mon_section_target_sales_amt
# Author: zsm
# Date: 2018/9/12 16:26
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_org_day_mon_section_target_sales_amt partition(day_date)
        select
            org_code
            , org_long_code
            , org_type
            , target_mon_section_sales_amt
            , current_timestamp as etl_time
            , day_date
        from {p_dm_schema}.dm_org_day_mon_section_target_sales_amt
        where
            day_date >= trunc('{p_input_date}','MM')--月初
            and day_date <= last_day('{p_input_date}')--月末
    '''
    spark.execute_sql(sql_insert)
