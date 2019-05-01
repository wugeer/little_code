# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_rst_skc_city_day_stock_analysis_toc
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

    # 创建tmp1表， 总铺货门店数
    sql_tmp1 = """
        select product_code
            , color_code
            , city_code
            , city_long_code
            , is_toc
            , has_available_stock_store_count as available_distributed_store_count
        from {p_dm_schema}.dm_skc_city_day_analysis_toc
        where day_date = '{p_input_date}'
    """

    # 城市总库存
    sql_tmp2 = """
        select product_code
            , color_code
            , city_code
            , city_long_code
            , is_toc
            , available_stock_qty
        from {p_dm_schema}.dm_skc_city_day_available_stock_toc
        where day_date = '{p_input_date}'
    """

    sql = """
        insert overwrite table {p_rst_schema}.rst_skc_city_day_stock_analysis_toc
        partition(day_date)
        select b.product_code
            , e.product_name
            , b.color_code
            , e.color_name
            , d.year_id as year
            , d.quarter_id as quarter 
            , a.available_distributed_store_count as total_distributed_store_count --总铺货门店数（加在途），也是店均库存分母
            , b.available_stock_qty as avg_store_stock_qty_numerator --店均库存分子（加在途后的总门店库存）
            , a.available_distributed_store_count as avg_store_stock_qty_denominator --总铺货门店数（加在途），也是店均库存分母
            , b.available_stock_qty as store_stock_qty       -- 可用库存 作为门店总库存
            , b.is_toc
            , b.city_code
            , b.city_long_code
            , g.org_code as region_code
            , g.org_longcode as region_long_code
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from tmp2 b 
        left join tmp1 a on a.product_code = b.product_code 
            and a.color_code = b.color_code
            and a.city_long_code = b.city_long_code
            and a.is_toc = b.is_toc
        inner join {p_edw_schema}.dim_product d on b.product_code = d.product_code
        inner join {p_edw_schema}.dim_product_skc e on b.product_code = e.product_code 
            and b.color_code = e.color_code
        inner join {p_edw_schema}.dim_stockorg f on b.city_long_code = f.org_longcode
        inner join {p_edw_schema}.dim_stockorg g on f.parent_id = g.org_id
    """

    tmp1 = foo.create_temp_table(sql_tmp1, 'tmp1')
    tmp2 = foo.create_temp_table(sql_tmp2, 'tmp2')

    foo.execute_sql(sql)

    foo.drop_temp_table('tmp1')
    foo.drop_temp_table('tmp2')
