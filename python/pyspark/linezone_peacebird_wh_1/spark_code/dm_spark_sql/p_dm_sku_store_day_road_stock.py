
# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_sku_store_day_road_stock
   Description :    计算目标产品sku在每个门店每天的在途库存,取的是抽取过来的太平鸟的在途库存   
   Author :       yangming
   date：          2018/9/3
-------------------------------------------------
   Change Activity:
                   2018/9/3:
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
    sql = """
        insert overwrite table {p_dm_schema}.dm_sku_store_day_road_stock
        partition(day_date)
        select a.product_code
            , a.color_code
            , a.size_code
            , a.org_code as store_code
            , sum(a.road_stock_qty) as road_stock_qty
            , current_timestamp as etl_time 
            , '{p_input_date}' as day_date
        from {p_edw_schema}.mid_sku_org_day_road_stock_peacebird a 
        inner join (select * from {p_edw_schema}.dim_target_store where is_store = 'Y') as ds
            on a.org_id = ds.store_id
        inner join (select * from {p_edw_schema}.dim_target_product where day_date = '{p_input_date}') as dtp 
            on a.product_id = dtp.product_id and ds.dq_long_code = dtp.dq_long_code
        where a.day_date = '{p_input_date}'
        group by a.product_code, a.color_code, a.size_code, a.org_code
    """

    foo.execute_sql(sql)