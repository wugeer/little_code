# -*- coding: utf-8 -*-

#  ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_org_day_mon_section_fct_sales_amt
# Author: zsm
# Date: 2018/9/12 16:16
# ----------------------------------------------------------------------------
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_org_day_mon_section_fct_sales_amt partition(day_date)
        select
            org_code
            , org_long_code
            , org_type
            , fct_mon_section_sales_amt
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_org_day_mon_section_fct_sales_amt
        where
            day_date = '{p_input_date}'
    '''
    spark.execute_sql(sql_insert)
