# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_mictiny_class_store_day
# Author: zsm
# Date: 2018/9/12 16:32
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)
    
    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_mictiny_class_store_day partition(day_date)
        select
            a.store_code
            , c.store_name
            , a.year_id as year
            , a.quarter_id as quarter
            , a.mictiny_class
            , a.stock_qty 
            , coalesce(b.total_sales_qty,0) as sales_qty
            , a.stock_amt
            , coalesce(b.total_sales_amt,0) as sales_amt
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_mictiny_class_store_day_stock a
        left join {p_dm_schema}.dm_mictiny_class_store_day_total_sales b
            on a.store_code=b.store_code
            and a.mictiny_class=b.mictiny_class
            and a.day_date=b.day_date
            and a.year_id=b.year_id
            and a.quarter_id=b.quarter_id
        inner join {p_edw_schema}.dim_store c
            on a.store_code=c.store_code
        where 
            a.day_date='{p_input_date}'
    '''
    spark.execute_sql(sql_insert)
