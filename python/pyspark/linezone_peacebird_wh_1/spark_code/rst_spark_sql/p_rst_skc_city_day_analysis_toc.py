# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_rst_skc_city_day_analysis_toc
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

    sql = """
        insert overwrite table {p_rst_schema}.rst_skc_city_day_analysis_toc
        partition(day_date)
        select a.product_code
            , dps.product_name 
            , a.color_code
            , dps.color_name
            , b.year_id as year 
            , b.quarter_id as quarter
            , b.band 
            , b.tiny_class
            , b.mictiny_class
            , p.prod_class
            , p.stage_lifetime
            , a.no_sales_no_available_stock_store_count
            , a.has_sales_no_available_stock_store_count
            , a.no_sales_available_fullsize_store_count
            , a.no_sales_available_brokensize_store_count
            , a.has_sales_available_brokensize_store_count
            , (a.has_sales_available_fullsize_store_count-coalesce(a.has_sales_fullsize_store_count, 0)) 
                as has_sales_available_fullsize_store_count
            , a.has_sales_fullsize_store_count
            , a.is_toc 
            , a.city_code
            , a.city_long_code
            , ds2.org_code as region_code
            , ds2.org_longcode as region_long_code
            , current_timestamp as etl_time 
            , '{p_input_date}' as day_date 
        from {p_dm_schema}.dm_skc_city_day_analysis_toc a
        inner join {p_edw_schema}.dim_product b on a.product_code = b.product_code
        inner join {p_edw_schema}.dim_product_skc dps on a.product_code = dps.product_code and a.color_code = dps.color_code
        left join (select * from {p_edw_schema}.mod_skc_week_classify 
            where cla_week_date = '{p_input_date_mon}') as p
            on dps.product_id = p.product_id and dps.color_id = p.color_id
        inner join {p_edw_schema}.dim_stockorg ds on a.city_long_code = ds.org_longcode
        inner join {p_edw_schema}.dim_stockorg ds2 on ds.parent_id = ds2.org_id
        where a.day_date = '{p_input_date}'
    """

    foo.execute_sql(sql)

