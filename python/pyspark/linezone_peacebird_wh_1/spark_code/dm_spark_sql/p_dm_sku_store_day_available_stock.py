# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_sku_store_day_available_stock
   Description : 计算目标产品sku在每个门店每天的可用库存（在库+在途）
   Author :       yangming
   date：          2018/9/4
-------------------------------------------------
   Change Activity:
                   2018/9/4:
-------------------------------------------------
"""

import os

from utils.tools import SparkInit

if __name__ == '__main__':
    # app_name
    file_name = os.path.basename(__file__)

    # 初始化spark环境
    foo = SparkInit(file_name)

    sql_tmp1 = """
            select b.product_code
                , b.color_code
                , b.size_code
                , c.store_code
                , '{p_input_date}' as day_date
                , stock_qty
            from {p_edw_schema}.mid_day_end_stock a
            inner join (select * from {p_edw_schema}.dim_target_store where is_store = 'Y') c on a.org_id = c.store_id
            inner join (select * from {p_edw_schema}.dim_target_product where day_date = '{p_input_date}') dtp 
              on a.product_id = dtp.product_id and c.dq_long_code = dtp.dq_long_code
            inner join {p_edw_schema}.dim_product_sku b 
              on a.product_id = b.product_id and a.color_id = b.color_id and a.size_id = b.size_id
            where stock_date = '{p_input_date}'
            union all 
            select product_code
                    , color_code
                    , size_code
                    , store_code
                    , day_date
                    , road_stock_qty as stock_qty
            from {p_dm_schema}.dm_sku_store_day_road_stock
            where day_date = '{p_input_date}'
    """

    tmp1 = foo.create_temp_table(sql_tmp1, 'tmp1')

    sql = """
            insert overwrite table {p_dm_schema}.dm_sku_store_day_available_stock
            partition(day_date)
            select product_code
                , color_code
                , size_code
                , store_code
                , sum(stock_qty) as available_stock_qty
                , current_timestamp as etl_time
                ,  '{p_input_date}' as day_date
            from tmp1
            group by product_code, color_code, size_code , store_code
    """

    foo.execute_sql(sql)
    
    foo.drop_temp_table('tmp1')
