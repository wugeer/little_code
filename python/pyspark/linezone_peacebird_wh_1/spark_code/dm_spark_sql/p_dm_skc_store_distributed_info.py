# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_skc_store_distributed_info
   Description :
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
            insert overwrite table {p_dm_schema}.dm_skc_store_distributed_info
            select
                stock.product_id
                , skc.product_code
                , stock.color_id
                , skc.color_code
                , stock.receive_org_id as store_id
                , receive.store_code as store_code
                , min(stock.send_date) as distri_send_day_date
                , min(stock.receive_date) as distri_receive_day_date
                , current_timestamp as etl_time
            from {p_edw_schema}.fct_stock as stock    
            inner join {p_edw_schema}.dim_product_skc as skc
                on stock.product_id = skc.product_id
                and stock.color_id = skc.color_id
            inner join (select store_id from {p_edw_schema}.dim_store where store_type='总仓') as send
                on stock.send_org_id = send.store_id
            inner join (select * from {p_edw_schema}.dim_store where is_store='Y' ) as receive
                on stock.receive_org_id = receive.store_id
            inner join (select * from {p_edw_schema}.dim_target_product where day_date='{p_input_date}') as target_product
                on target_product.product_id = skc.product_id
                and target_product.dq_id = receive.dq_id 
            where stock.send_date <= '{p_input_date}'
            group by stock.product_id, skc.product_code, stock.color_id, skc.color_code, 
                stock.receive_org_id, receive.store_code
    """

    foo.execute_sql(sql)
