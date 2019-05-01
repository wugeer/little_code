# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_rst_skc_add_order
   Description : 追单部分
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
        insert overwrite table {p_rst_schema}.rst_skc_add_order
        select c.product_code
            , c.product_name
            , c.color_code
            , c.color_name
            , a.order_date as add_order_date
            , b.expected_arrive_date as expected_arrival_date
            , sum(a.add_order_qty) as add_order_qty
            , b.actual_arrive_date as actual_arrival_date
            , sum(a.arrived_qty) as receive_qty
            , current_timestamp as etl_time
        from {p_edw_schema}.mid_sku_add_order_info as a 
        inner join {p_edw_schema}.mid_sku_add_order_expected_arrival as b 
            on a.order_id = b.order_id
            and a.sku_id = b.sku_id 
        inner join {p_edw_schema}.dim_product_sku as c 
            on a.sku_id = c.sku_id
        group by
            c.product_code
            , c.product_name
            , c.color_code
            , c.color_name
            , a.order_date
            , b.expected_arrive_date
            , b.actual_arrive_date
    """

    foo.execute_sql(sql)
