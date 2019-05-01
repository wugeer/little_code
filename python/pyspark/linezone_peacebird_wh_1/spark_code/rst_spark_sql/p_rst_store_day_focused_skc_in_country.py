# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_store_day_focused_skc_in_country
# Author: zsm
# Date: 2018/9/12 15:50
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_store_day_focused_skc_in_country partition(day_date)		
        select
            stock.product_code
            , skc.product_name
            , stock.color_code
            , skc.color_name
            , focus.store_code
            , pro.year_id as year
            , pro.quarter_id as quarter
            , sales.total_sales_qty / 
                (sales.total_sales_qty+stock.warehouse_total_stock_qty+stock.store_total_stock_qty+road.road_stock_qty) as sale_out_rate
            , brokensize.brokensize_rate
            , sales.total_sales_qty / 
                (sales.total_sales_qty+stock.warehouse_total_stock_qty+stock.store_total_stock_qty) as sale_inventory_rate
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_store_day_expection_focused_skc as focus
        inner join {p_dm_schema}.dm_skc_country_day_stock as stock
            on focus.product_code = stock.product_code
            and focus.color_code = stock.color_code
            and focus.day_date = stock.day_date
        inner join {p_dm_schema}.dm_skc_country_day_sales as sales
            on stock.product_code = sales.product_code
            and stock.color_code = sales.color_code
            and stock.day_date = sales.day_date
        inner join {p_dm_schema}.dm_skc_country_day_road_stock as road
            on stock.product_code = road.product_code
            and stock.color_code = road.color_code
            and stock.day_date = road.day_date
        inner join {p_dm_schema}.dm_skc_country_day_brokensize_rate as brokensize
            on stock.product_code = brokensize.product_code
            and stock.color_code = brokensize.color_code
            and stock.day_date = brokensize.day_date
        inner join {p_edw_schema}.dim_product_skc as skc
            on stock.product_code = skc.product_code
            and stock.color_code = skc.color_code
        inner join {p_edw_schema}.dim_product as pro
            on stock.product_code = pro.product_code
        where 
            stock.day_date = '{p_input_date}'
    '''
    spark.execute_sql(sql_insert)
