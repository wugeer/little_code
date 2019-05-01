# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_skc_city_day_available_stock_toc
# Author: zsm
# Date: 2018/9/10 13:22
# ----------------------------------------------------------------------------
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql = '''
        insert overwrite table {p_dm_schema}.dm_skc_city_day_available_stock_toc partition(day_date)
        select a.product_code
            , a.color_code
            , max(b.city_code) as city_code
            , b.city_long_code as city_long_code
            , b.is_toc as is_toc
            , sum(a.available_stock_qty) as available_stock_qty
            , current_timestamp as etl_time 
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_sku_store_day_available_stock a 
        inner join {p_edw_schema}.dim_store b 
            on a.store_code = b.store_code
        where 
            day_date = '{p_input_date}'
        group by 
            a.product_code
            , a.color_code
            , b.city_long_code
            , b.is_toc
    '''
    spark.execute_sql(sql)