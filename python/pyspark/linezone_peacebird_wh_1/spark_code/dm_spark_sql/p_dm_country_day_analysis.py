# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_country_day_analysis
# Author: zsm
# Date: 2018/9/11 18:22
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_tmp = '''
        select
            b.year_id as year_id
            , b.quarter_id as quarter_id
            , '{p_input_date}' as day_date
            , sum(a.total_sales_qty) as total_sales_qty
            , 0 as road_stock_qty
            , 0 as warehouse_total_stock_qty
            , 0 as store_total_stock_qty
            , (cast(null as int)) as store_count
            , (cast(null as int)) as brokensize_store_count
            , current_timestamp as etl_time 
        from {p_dm_schema}.dm_skc_store_day_sales a
        inner join {p_edw_schema}.dim_product b    -- 取年份、季节
            on a.product_code=b.product_code
        where day_date = '{p_input_date}'
        group by b.year_id, b.quarter_id
        
        union ALL 
        
        select 
            b.year_id
            , b.quarter_id
            , '{p_input_date}' as day_date
            , 0 as total_sales_qty
            , sum(a.road_stock_qty) as road_stock_qty
            , 0 as warehouse_total_stock_qty
            , 0 as store_total_stock_qty
            , (cast(null as int)) as store_count
            , (cast(null as int)) as brokensize_store_count
            , current_timestamp as etl_time  
        from {p_edw_schema}.mid_sku_org_day_road_stock_peacebird a -- 取在途库存road_stock_qty（不仅包括门店的在途库存还包括总仓的）
        inner join (
            select * from {p_edw_schema}.dim_store where (is_store='Y' and store_type='自营') or store_code='CB37'
        ) as ds
            on a.org_id = ds.store_id
        inner join tmp_dim_target_product b    -- 取年份、季节
            on a.product_id=b.product_id
        where a.day_date = '{p_input_date}'
        group by b.year_id, b.quarter_id
        
        union ALL 
        
        select 
            b.year_id
            , b.quarter_id
            , '{p_input_date}' as day_date
            , 0 as total_sales_qty
            , 0 as road_stock_qty
            , sum(a.warehouse_total_stock_qty) as warehouse_total_stock_qty
            , sum(a.store_total_stock_qty) as store_total_stock_qty
            , (cast(null as int)) as store_count
            , (cast(null as int))as brokensize_store_count
            , current_timestamp as etl_time 
        from {p_dm_schema}.dm_skc_country_day_stock a
        inner join {p_edw_schema}.dim_product b    -- 取年份、季节
            on a.product_code=b.product_code
        where day_date = '{p_input_date}'
        group by b.year_id, b.quarter_id
        
        union ALL 
        
        select 
             b.year_id
            , b.quarter_id
            , '{p_input_date}' as day_date
            , 0 as total_sales_qty
            , 0 as road_stock_qty
            , 0 as warehouse_total_stock_qty
            , 0 as store_total_stock_qty
            , count(1) as store_count
            , sum(case
                        when a.available_is_fullsize = 'N' then 1
                        else 0
                  end) as brokensize_store_count
            , current_timestamp as etl_time
        from {p_dm_schema}.dm_skc_store_day_available_fullsize a
        inner join {p_edw_schema}.dim_product b    -- 取年份、季节
            on a.product_code=b.product_code
        inner join (
            select store_code from {p_edw_schema}.dim_store where is_store = 'Y' and status='正常'
        ) c 
            on a.store_code = c.store_code
        where day_date = '{p_input_date}'
        group by b.year_id, b.quarter_id
    '''

    sql_tmp_dim_target_product = '''
        select
            product_id
            , year_id
            , quarter_id 
        from {p_edw_schema}.dim_target_product
        where day_date = '{p_input_date}'
        group by product_id, year_id, quarter_id
    '''
    
    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_country_day_analysis partition(day_date)
        select
            a.year_id
            , a.quarter_id
            , sum(a.total_sales_qty) as total_sales_qty
            , sum(a.road_stock_qty) as road_stock_qty
            , sum(a.warehouse_total_stock_qty) as warehouse_total_stock_qty
            , sum(a.store_total_stock_qty) as store_total_stock_qty
            , sum(a.store_count) as store_count
            , sum(a.brokensize_store_count) as brokensize_store_count
            , current_timestamp as etl_time 
            , '{p_input_date}' as day_date
        from tmp a
        group by 
            a.year_id
            , a.quarter_id
    '''

    spark.create_temp_table(sql_tmp_dim_target_product, 'tmp_dim_target_product')
    spark.create_temp_table(sql_tmp, 'tmp')
    spark.execute_sql(sql_insert)
    spark.drop_temp_table('tmp')
    spark.drop_temp_table('tmp_dim_target_product')
