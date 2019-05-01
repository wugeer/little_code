# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_rst_skc_product_tag_toc
   Description :
   Author :       yangming
   date：          2018/9/12
-------------------------------------------------
   Change Activity:
                   2018/9/12:
-------------------------------------------------
"""

import os

from utils.tools import SparkInit

if __name__ == '__main__':
    # app_name
    file_name = os.path.basename(__file__)

    # 初始化spark环境
    foo = SparkInit(file_name)

    # 创建analysis_tmp
    sql_analysis_tmp = """
        select a.product_code
            , a.color_code
            , a.is_toc
            , a.city_long_code
            , coalesce(a.has_sales_available_brokensize_store_count,0) 
                + coalesce(no_sales_available_brokensize_store_count,0) as brokensize_rate_numerator
            , coalesce(a.no_sales_has_available_stock_store_count,0) as no_sales_store_rate_numerator
            , coalesce(a.has_sales_available_brokensize_store_count,0) 
                + coalesce(a.has_sales_available_fullsize_store_count,0)
                - coalesce(a.has_sales_fullsize_store_count,0) as sales_brokensize_rate_numerator
            , coalesce(a.has_available_stock_store_count,0) as has_available_stock_store_count
            , coalesce(has_sales_available_brokensize_store_count,0)
                + coalesce(has_sales_available_fullsize_store_count,0) as sales_brokensize_rate_denominator
        from {p_dm_schema}.dm_skc_city_day_analysis_toc a 
        where day_date = '{p_input_date}'
    """

    # 创建sales_tmp
    sql_sales_tmp = """
        select a.product_code
            , a.color_code
            , b.is_toc
            , b.city_long_code
            , sum(a.total_sales_qty) as total_sales_qty
            , sum(a.last_week_sales_qty) as last_week_sales_qty
        from {p_dm_schema}.dm_skc_store_day_sales a 
        inner join {p_edw_schema}.dim_store b on a.store_code = b.store_code
        where a.day_date = '{p_input_date}'
        group by a.product_code,a.color_code,b.city_long_code, b.is_toc
    """

    # 创建stock_tmp
    sql_stock_tmp = """
        select a.product_code
            , a.color_code
            , a.is_toc
            , a.city_long_code
            , a.available_stock_qty as store_total_stock_qty
        from {p_dm_schema}.dm_skc_city_day_available_stock_toc a 
        where a.day_date = '{p_input_date}'
    """

    sql = """
        insert overwrite table {p_rst_schema}.rst_skc_product_tag_toc
        partition(day_date)
        select skc.product_code
            , skc.product_name 
            , skc.color_code
            , skc.color_name
            , p.year_id as year
            , p.quarter_id as quarter
            , p.band
            , p.tiny_class
            , p.mictiny_class
            , stock_tmp.is_toc
            , city.org_code as city_code
            , city.org_longcode as city_long_code
            , dq.org_code as region_code
            , dq.org_longcode as region_long_code
            , coalesce(analysis_tmp.brokensize_rate_numerator,0) as brokensize_rate_numerator --断码率分子
            , coalesce(analysis_tmp.has_available_stock_store_count,0) 
                as brokensize_rate_denominator              --断码率分母，销售断码率分母，无销售门店比例分母，铺货率分子
            , coalesce(sales_tmp.total_sales_qty,0) as delivery_sales_rate_numerator --发销率分子 
            , coalesce(sales_tmp.total_sales_qty,0) + coalesce(stock_tmp.store_total_stock_qty,0) 
                as delivery_sales_rate_denominator          --发销率分母
            , coalesce(analysis_tmp.sales_brokensize_rate_numerator,0) as sales_brokensize_rate_numerator --销售断码率分子
            , coalesce(analysis_tmp.sales_brokensize_rate_denominator,0) as sales_brokensize_rate_denominator --销售断码率分母
            , coalesce(stock_tmp.store_total_stock_qty,0) as store_turnover_weeks_numerator--库存周转周数分子
            , sales_tmp.last_week_sales_qty as store_turnover_weeks_denominator --库存周转周数分母，没有指标就为空，不赋值为零
            , coalesce(analysis_tmp.no_sales_store_rate_numerator,0) AS no_sales_store_rate_numerator--无销售门店比例分子
            , coalesce(analysis_tmp.has_available_stock_store_count,0) as no_sales_store_rate_denominator --无销售门店比例分母
            , coalesce(analysis_tmp.has_available_stock_store_count,0) as distribute_rate_numerator --铺货率分子
            , null as distribute_rate_denominator --铺货率分母
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date 
        from stock_tmp  
        left join analysis_tmp on analysis_tmp.product_code = stock_tmp.product_code
            and analysis_tmp.color_code = stock_tmp.color_code
            and analysis_tmp.city_long_code = stock_tmp.city_long_code
            and analysis_tmp.is_toc = stock_tmp.is_toc
        left join sales_tmp on analysis_tmp.product_code = sales_tmp.product_code
            and analysis_tmp.color_code = sales_tmp.color_code
            and analysis_tmp.city_long_code = sales_tmp.city_long_code
            and analysis_tmp.is_toc = sales_tmp.is_toc
        inner join {p_edw_schema}.dim_product as p on stock_tmp.product_code = p.product_code
        inner join {p_edw_schema}.dim_product_skc skc on stock_tmp.product_code = skc.product_code
            and stock_tmp.color_code = skc.color_code
        inner join {p_edw_schema}.dim_stockorg as city on stock_tmp.city_long_code = city.org_longcode
        inner join {p_edw_schema}.dim_stockorg as dq on city.parent_id = dq.org_id
    """

    analysis_tmp = foo.create_temp_table(sql_analysis_tmp, 'analysis_tmp')
    sales_tmp = foo.create_temp_table(sql_sales_tmp, 'sales_tmp')
    stock_tmp = foo.create_temp_table(sql_stock_tmp, 'stock_tmp')

    foo.execute_sql(sql)

    foo.drop_temp_table('analysis_tmp')
    foo.drop_temp_table('sales_tmp')
    foo.drop_temp_table('stock_tmp')
