# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_rst_skc_city_day_analysis_for_order_toc
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

    # 创建tmp1表， 先取出断码率的分子 分母
    sql_tmp1 = """
        select product_code
            , color_code
            , city_code
            , city_long_code
            , is_toc
            , available_brokensize_store_count as brokensize_numerator
            , has_available_stock_store_count as brokensize_denominator
        from {p_dm_schema}.dm_skc_city_day_analysis_toc 
        where day_date = '{p_input_date}'
    """

    # 再取出销量相关的数据
    sql_tmp2 = """
        select product_code
            , color_code
            , city_code
            , city_long_code
            , is_toc
            , last_week_sales_qty
            , week_sales_qty
            , total_sales_qty
        from {p_dm_schema}.dm_skc_city_day_sales_toc 
        where day_date = '{p_input_date}'
    """

    sql = """
        insert overwrite table {p_rst_schema}.rst_skc_city_day_analysis_for_order_toc
        partition(day_date)
        select a.product_code
            , c.product_name
            , a.color_code
            , c.color_name
            , a.brokensize_numerator
            , a.brokensize_denominator
            , b.last_week_sales_qty
            , b.week_sales_qty
            , b.total_sales_qty
            , a.is_toc
            , a.city_code
            , a.city_long_code
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from tmp1 a 
        full outer join tmp2 b on a.product_code = b.product_code
            and a.color_code = b.color_code and a.city_long_code = b.city_long_code
            and a.is_toc = b.is_toc
        inner join {p_edw_schema}.dim_product_skc c on a.product_code = c.product_code
            and a.color_code = c.color_code
    """

    tmp1 = foo.create_temp_table(sql_tmp1, 'tmp1')
    tmp2 = foo.create_temp_table(sql_tmp2, 'tmp2')

    foo.execute_sql(sql)

    foo.drop_temp_table('tmp1')
    foo.drop_temp_table('tmp2')
