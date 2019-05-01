# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_skc_store_day_fullprice
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
            insert overwrite table {p_dm_schema}.dm_skc_store_day_fullprice
            partition(day_date)
            select
                c.product_code
                , c.color_code
                , d.store_code
                , sum(case when a.sale_date = '{p_input_date}' 
                                then a.qty else 0 end) as fullprice_qty
                , sum(a.qty) as total_fullprice_qty
                , current_timestamp as etl_time
                , '{p_input_date}' as day_date
            from {p_edw_schema}.fct_sales a
            inner join {p_edw_schema}.dim_target_store d    -- 排除 加盟门店 的 销售数据
                on a.store_id = d.store_id 
            inner join {p_edw_schema}.dim_product_sku c 
                on a.product_id = c.product_id 
                    and a.color_id = c.color_id and a.size_id = c.size_id            
            inner join (select * from {p_edw_schema}.dim_target_product where day_date = '{p_input_date}') b 
                on a.product_id = b.product_id and d.dq_long_code = b.dq_long_code
            where a.sale_date <= '{p_input_date}'
                and (a.real_amt/(a.qty*b.tag_price))>={p_full_price_rate}
            group by c.product_code, c.color_code, d.store_code
    """

    foo.execute_sql(sql)