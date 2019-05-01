# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_skc_country_day_expection_analysis
# Author: zsm
# Date: 2018/9/12 15:07
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_skc_country_day_expection_analysis partition(day_date)
        select 
            pro.year_id as year
            , pro.quarter_id as quarter
            , a.product_code
            , skc.product_name
            , a.color_code
            , skc.color_name
            , (case when a.pre_class = 'high' then 1 when a.pre_class = 'low' then -1 else 0 end) as pre_class
            , a.sales_out_rate as sale_out_rate
            , b.brokensize_rate  -- 断码率
            , (case 
                when(a.store_total_stock_qty + a.road_stock_qty + a.total_sales_qty) <> 0 
                    then (a.total_sales_qty*1.0 / (a.store_total_stock_qty + a.road_stock_qty + a.total_sales_qty)) 
                else null 
            end) as sale_inventory_rate  -- 发销率=累积销量/累积销量+总仓库存+门店库存
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_skc_country_day_pre_classify a
        inner join {p_dm_schema}.dm_skc_country_day_brokensize_rate b
            on a.product_code = b.product_code 
            and a.color_code = b.color_code
            and a.day_date = b.day_date
        inner join {p_edw_schema}.dim_product as pro
            on a.product_code = pro.product_code
        inner join {p_edw_schema}.dim_product_skc as skc
            on a.product_code = skc.product_code
            and a.color_code = skc.color_code
        where 
            a.day_date = '{p_input_date}'
            and a.pre_class <> 'conform'   --重点关注商品排除掉预期内
    '''

    spark.execute_sql(sql_insert)
