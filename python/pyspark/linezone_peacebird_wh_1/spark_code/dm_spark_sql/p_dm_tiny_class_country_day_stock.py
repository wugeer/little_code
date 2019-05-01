# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_tiny_class_country_day_stock
# Author: zsm
# Date: 2018/9/11 11:44
# ----------------------------------------------------------------------------
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_stock = '''
        select b.year_id 
            , b.quarter_id
            , b.tiny_class
            , sum(a.warehouse_total_stock_qty) as warehouse_total_stock_qty
            , sum(a.store_total_stock_qty) as store_total_stock_qty
            , 0 as road_total_stock_qty
        from {p_dm_schema}.dm_skc_country_day_stock a 
        inner join {p_edw_schema}.dim_product b 
            on a.product_code = b.product_code
        where 
            a.day_date = '{p_input_date}'
        group by 
            b.year_id
            , b.quarter_id
            , b.tiny_class
            
        union all
        
        select b.year_id
            , b.quarter_id
            , b.tiny_class
            , 0 as warehouse_total_stock_qty
            , 0 as store_total_stock_qty
            , sum(a.road_stock_qty) as road_total_stock_qty 
        from {p_dm_schema}.dm_skc_country_day_road_stock a 
        inner join {p_edw_schema}.dim_product b 
            on a.product_code = b.product_code
        where 
            a.day_date = '{p_input_date}'
        group by 
            b.year_id
            , b.quarter_id
            , b.tiny_class
    '''

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_tiny_class_country_day_stock partition(day_date)
        select year_id 
            , quarter_id
            , tiny_class
            , sum(warehouse_total_stock_qty) as warehouse_total_stock_qty
            , sum(store_total_stock_qty) as store_total_stock_qty
            , sum(road_total_stock_qty) as road_total_stock_qty
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from stock
        group by 
            year_id
            , quarter_id
            , tiny_class
    '''
    spark.create_temp_table(sql_stock, 'stock')
    spark.execute_sql(sql_insert)
