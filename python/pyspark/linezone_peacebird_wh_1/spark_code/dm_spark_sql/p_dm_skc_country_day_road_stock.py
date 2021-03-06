# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_skc_country_day_road_stock
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
    sql = """          
            insert overwrite table {p_dm_schema}.dm_skc_country_day_road_stock
            partition(day_date)
            select dsdrs.product_code
                , dsdrs.color_code
                , sum(dsdrs.road_stock_qty) as road_stock_qty
                , current_timestamp as etl_time
                , '{p_input_date}' as day_date
            from {p_dm_schema}.dm_skc_store_day_road_stock dsdrs
            where dsdrs.day_date = '{p_input_date}'
            group by dsdrs.product_code, dsdrs.color_code
    """

    foo.execute_sql(sql)