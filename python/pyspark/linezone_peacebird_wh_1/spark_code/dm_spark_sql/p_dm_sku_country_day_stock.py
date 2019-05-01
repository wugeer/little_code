# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_sku_country_day_stock
# Author: zsm
# Date: 2018/9/11 9:50
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_target_pro = '''
        select product_id
        from {p_edw_schema}.dim_target_product 
        where day_date = '{p_input_date}'
        group by product_id
    '''

    sql_stock = '''
        select c.product_code
            , c.color_code
            , c.size_code
            , 0 as warehouse_total_stock_qty
            , sum(a.stock_qty) store_total_stock_qty
        from {p_edw_schema}.mid_day_end_stock a 
        inner join (select store_id from {p_edw_schema}.dim_target_store where is_store = 'Y') b 
            on a.org_id = b.store_id
        inner join {p_edw_schema}.dim_product_sku c 
            on a.product_id = c.product_id 
            and a.color_id = c.color_id 
            and a.size_id = c.size_id
        inner join target_pro d 
            on a.product_id=d.product_id
        where 
            stock_date = '{p_input_date}'
        group by 
            c.product_code
            , c.color_code
            , c.size_code
        
        union ALL 
        
        select c.product_code
            , c.color_code
            , c.size_code
            , sum(a.stock_qty) as warehouse_total_stock_qty
            , 0 as store_total_stock_qty
        from {p_edw_schema}.mid_day_end_stock a 
        inner join (select store_id from {p_edw_schema}.dim_target_store where store_code = 'CB37') b 	-- 只取出总仓
            on a.org_id = b.store_id
        inner join {p_edw_schema}.dim_product_sku c 
            on a.product_id = c.product_id 
            and a.color_id = c.color_id 
            and a.size_id = c.size_id
        inner join target_pro d 
            on a.product_id = d.product_id
        where 
            stock_date = '{p_input_date}'
        group by 
            c.product_code
            , c.color_code
            , c.size_code
    '''

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_sku_country_day_stock partition(day_date)
        select product_code
            , color_code
            , size_code
            , sum(warehouse_total_stock_qty) as warehouse_total_stock_qty
            , sum(store_total_stock_qty) as store_total_stock_qty
            , current_timestamp as etl_time 
            , '{p_input_date}' as day_date
        from stock 
        group by 
            product_code
            , color_code
            , size_code
    '''

    spark.create_temp_table(sql_target_pro, 'target_pro')
    spark.create_temp_table(sql_stock, 'stock')
    spark.execute_sql(sql_insert)
    spark.drop_temp_table('target_pro')
    spark.drop_temp_table('stock')
