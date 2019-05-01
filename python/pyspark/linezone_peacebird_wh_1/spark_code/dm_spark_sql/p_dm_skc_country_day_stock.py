# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_skc_country_day_stock
   Description :
   Author :       yangming
   date：          2018/9/10
-------------------------------------------------
   Change Activity:
                   2018/9/10:
-------------------------------------------------
"""
import os

from utils.tools import SparkInit

if __name__ == '__main__':
    # app_name
    file_name = os.path.basename(__file__)

    # 初始化spark环境
    foo = SparkInit(file_name)

    # sql部分
    sql_tmp0 = """          
        select product_id
        from {p_edw_schema}.dim_target_product 
        where day_date = '{p_input_date}'
        group by product_id
    """

    # 先计算门店总库存 和 总仓库存
    sql_tmp1 = """
        select c.product_code
            , c.color_code
            , 0 as warehouse_total_stock_qty
            , sum(a.stock_qty) store_total_stock_qty
        from {p_edw_schema}.mid_day_end_stock a
        inner join (select store_id from {p_edw_schema}.dim_target_store where is_store = 'Y') b on a.org_id = b.store_id
        inner join {p_edw_schema}.dim_product_skc c on a.product_id = c.product_id and a.color_id = c.color_id
        inner join tmp0 d on a.product_id = d.product_id
        where a.stock_date = '{p_input_date}'
        group by c.product_code, c.color_code
        union all 
        select c.product_code
            , c.color_code
            , sum(a.stock_qty) as warehouse_total_stock_qty
            , 0 as store_total_stock_qty
        from {p_edw_schema}.mid_day_end_stock a
        inner join (select store_id from {p_edw_schema}.dim_target_store where store_code = 'CB37') b on a.org_id = b.store_id
        inner join {p_edw_schema}.dim_product_skc c on a.product_id = c.product_id and a.color_id = c.color_id
        inner join tmp0 d on a.product_id = d.product_id
        where stock_date = '{p_input_date}'
        group by c.product_code, c.color_code
    """

    foo.create_temp_table(sql_tmp0, 'tmp0')
    foo.create_temp_table(sql_tmp1, 'tmp1')

    sql = """
        insert overwrite table {p_dm_schema}.dm_skc_country_day_stock
        partition(day_date)
        select product_code
            , color_code
            , sum(warehouse_total_stock_qty) as warehouse_total_stock_qty
            , sum(store_total_stock_qty) as store_total_stock_qty
            ,current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from tmp1
        group by product_code, color_code
    """
    
    foo.execute_sql(sql)
    
    foo.drop_temp_table('tmp0')
    foo.drop_temp_table('tmp1')