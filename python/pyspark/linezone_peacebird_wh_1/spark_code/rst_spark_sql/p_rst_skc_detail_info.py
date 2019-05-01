# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_skc_detail_info
# Author: zsm
# Date: 2018/9/12 18:07
# ----------------------------------------------------------------------------
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_stock_1 = '''
        select a.product_code
            , a.color_code
            , (case 
                when a.size_code in('2','3','4','5','6','7') 
                    then a.size_code 
                else '100' 
            end) as size_code
            , (case 
                when p.size_name in('S','M','L','XL','XXL','XXXL','s','m','l','xl','xxl','xxxl') 
                    then p.size_name 
                else 'other' 
            end) as size_name
            , warehouse_total_stock_qty
            , store_total_stock_qty
            , '{p_input_date}'as day_date
        from {p_dm_schema}.dm_sku_country_day_stock a 
        inner join {p_edw_schema}.dim_product_sku p 
            on a.product_code = p.product_code 
            and a.color_code = p.color_code 
            and a.size_code = p.size_code
        where 
            a.day_date = '{p_input_date}'       
    '''

    sql_stock_2 = '''
        select product_code
            , color_code
            , size_name
            , concat(size_name,'+',sum(warehouse_total_stock_qty),'+',size_code) as warehouse_total_stock_qty
            , sum(warehouse_total_stock_qty) as warehouse_total_stock_qty_int
            , concat(size_name,'+',sum(store_total_stock_qty),'+',size_code) as store_total_stock_qty
            , sum(store_total_stock_qty) as store_total_stock_qty_int
        from stock_1
        group by 
            product_code
            , color_code
            , size_name
            , size_code      
    '''

    sql_stock_3 = '''
        select product_code
            , color_code
            , concat_ws(',',collect_set(warehouse_total_stock_qty)) as warehouse_total_stock_qty
            , sum(warehouse_total_stock_qty_int) as warehouse_total_stock_qty_int
            , concat_ws(',',collect_set(store_total_stock_qty)) as store_total_stock_qty
            , sum(store_total_stock_qty_int) as store_total_stock_qty_int
        from stock_2
        group by 
            product_code
            , color_code      
    '''

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_skc_detail_info partition(day_date)
        select distinct skc.product_code
            , skc.product_name
            , skc.color_code
            , skc.color_name
            , p.year_id as year
            , p.quarter_id as quarter
            , p.band
            , p.tiny_class
            , mod.prod_class as product_class
            , null as material
            , stock_3.warehouse_total_stock_qty as stock_qty
            , stock_3.warehouse_total_stock_qty_int as stock_qty_int
            , stock_3.store_total_stock_qty as store_stock_qty
            , stock_3.store_total_stock_qty_int as store_stock_qty_int
            , cycle.distributed_days
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from stock_3 
        left join (
            select * from {p_dm_schema}.dm_skc_country_day_life_cycle 
            where day_date = '{p_input_date}'
        ) as cycle 
            on stock_3.product_code = cycle.product_code 
            and stock_3.color_code = cycle.color_code 
        inner join {p_edw_schema}.dim_product p 
            on stock_3.product_code = p.product_code
        inner join {p_edw_schema}.dim_product_skc as skc 
            on stock_3.product_code = skc.product_code 
            and stock_3.color_code = skc.color_code
        left join (
            select * from {p_edw_schema}.mod_skc_week_classify 
            where cla_week_date = '{p_input_date_mon}'
        ) as mod 
            on skc.product_id = mod.product_id 
            and skc.color_id = mod.color_id     
    '''

    spark.create_temp_table(sql_stock_1, 'stock_1')
    spark.create_temp_table(sql_stock_2, 'stock_2')
    spark.create_temp_table(sql_stock_3, 'stock_3')
    spark.execute_sql(sql_insert)
    spark.drop_temp_table('stock_1')
    spark.drop_temp_table('stock_2')
    spark.drop_temp_table('stock_3')
