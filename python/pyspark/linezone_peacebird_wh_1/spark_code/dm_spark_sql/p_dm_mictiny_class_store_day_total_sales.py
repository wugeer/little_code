# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_mictiny_class_store_day_total_sales
# Author: zsm
# Date: 2018/9/11 18:47
# ----------------------------------------------------------------------------
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_mictiny_class_store_day_total_sales partition(day_date)
        select 
            b.mictiny_class as mictiny_class
            , a.store_code
            , sum(a.last_seven_days_sales_qty) total_sales_qty
            , sum(a.last_seven_days_sales_amt) total_sales_amt
            , current_timestamp as etl_time
            , b.year_id
            , b.quarter_id
            , '{p_input_date}' as day_date 
        from {p_dm_schema}.dm_skc_store_day_sales a
        inner join {p_edw_schema}.dim_product b on a.product_code=b.product_code
        where a.day_date='{p_input_date}'
        group by b.mictiny_class, b.year_id, b.quarter_id, a.store_code
    '''
    spark.execute_sql(sql_insert)

