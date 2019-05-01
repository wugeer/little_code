# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_skc_classify_band_country_day_stock
# Author: zsm
# Date: 2018/9/11 17:26
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_stock = '''
        select p.prod_class
            , p.stage_lifetime
            , b.year_id
            , b.quarter_id
            , b.band
            , sum(a.warehouse_total_stock_qty) as warehouse_total_stock_qty
            , sum(a.store_total_stock_qty) as store_total_stock_qty
            , 0 as road_total_stock_qty
        from {p_dm_schema}.dm_skc_country_day_stock a 
        inner join {p_edw_schema}.dim_product b 
            on a.product_code = b.product_code
        inner join {p_edw_schema}.dim_product_skc as dps 
            on a.product_code = dps.product_code 
            and a.color_code = dps.color_code
        inner join (
            select * from {p_edw_schema}.mod_skc_week_classify where cla_week_date = '{p_input_date_mon}' 
        ) as p
            on dps.product_id = p.product_id 
            and dps.color_id = p.color_id
        where 
            day_date = '{p_input_date}'
        group by 
            p.prod_class
            , p.stage_lifetime
            , b.year_id
            , b.quarter_id
            , b.band
         
         union ALL 
         
        select p.prod_class
            , p.stage_lifetime
            , b.year_id
            , b.quarter_id
            , b.band
            , 0 as warehouse_total_stock_qty
            , 0 as store_total_stock_qty
            , sum(a.road_stock_qty) as road_total_stock_qty 
        from {p_dm_schema}.dm_skc_country_day_road_stock a 
        inner join {p_edw_schema}.dim_product b 
            on a.product_code = b.product_code
        inner join {p_edw_schema}.dim_product_skc as dps 
            on a.product_code = dps.product_code 
            and a.color_code = dps.color_code
        inner join (
            select * from {p_edw_schema}.mod_skc_week_classify where cla_week_date = '{p_input_date_mon}'
        ) as p
            on dps.product_id = p.product_id 
            and dps.color_id = p.color_id
        where 
            day_date = '{p_input_date}'
        group by 
            p.prod_class
            , p.stage_lifetime
            , b.year_id
            , b.quarter_id, b.band
    '''

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_skc_classify_band_country_day_stock partition(day_date)
        select prod_class
            , stage_lifetime
            , year_id
            , quarter_id
            , band
            , sum(warehouse_total_stock_qty) as warehouse_total_stock_qty
            , sum(store_total_stock_qty) as store_total_stock_qty
            , sum(road_total_stock_qty) as road_total_stock_qty
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from stock
        group by 
            prod_class
            , stage_lifetime
            , year_id
            , quarter_id, band
    '''
    spark.create_temp_table(sql_stock, 'stock')
    spark.execute_sql(sql_insert)
    spark.drop_temp_table('stock')
