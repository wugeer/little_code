# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_skc_store_day_stock
   Description :  计算skc在门店的库存
   Author :       yangming
   date：          2018/9/7
-------------------------------------------------
   Change Activity:
                   2018/9/7:
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
            insert overwrite table {p_dm_schema}.dm_skc_store_day_stock
            partition(day_date)
            select dps.product_code
                , dps.color_code
                , ds.store_code
                , sum(mdes.stock_qty) as stock_qty
                , current_timestamp as etl_time
                , '{p_input_date}' as day_date
            from {p_edw_schema}.mid_day_end_stock as mdes 
            inner join (select * from {p_edw_schema}.dim_target_store where is_store = 'Y') as ds on mdes.org_id = ds.store_id    --只取出 自营 门店
            inner join (select * from {p_edw_schema}.dim_target_product where day_date = '{p_input_date}') b 
                on mdes.product_id = b.product_id and ds.dq_long_code = b.dq_long_code
            inner join {p_edw_schema}.dim_product_skc as dps on mdes.product_id = dps.product_id and mdes.color_id = dps.color_id
            where mdes.stock_date ='{p_input_date}'                     
            group by dps.product_code, dps.color_code, ds.store_code
    """

    foo.execute_sql(sql)