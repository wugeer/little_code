# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_store_day_focused_skc_in_store
# Author: zsm
# Date: 2018/9/12 15:43
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_store_day_focused_skc_in_store partition(day_date)		
        select
            b.product_code
            ,g.product_name
            ,b.color_code
            ,g.color_name
            ,b.store_code
            ,f.year_id as year
            ,f.quarter_id as quarter
            ,coalesce(a.last_two_week_sales_qty,0) as last_two_week_sales_qty
            ,b.stock_qty
            ,a.is_fullsize
            ,current_timestamp as etl_time
            ,'{p_input_date}' as day_date
        from {p_dm_schema}.dm_store_day_expection_focused_skc a
        inner join {p_dm_schema}.dm_skc_store_day_stock b
            on a.store_code=b.store_code
            and a.product_code=b.product_code
            and a.color_code=b.color_code
            and a.day_date=b.day_date
        inner join {p_edw_schema}.dim_product f
            on b.product_code=f.product_code
        inner join {p_edw_schema}.dim_product_skc g
            on b.product_code=g.product_code
            and b.color_code=g.color_code
        where b.day_date='{p_input_date}'
    '''
    spark.execute_sql(sql_insert)
