# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_skc_country_add_order
# Author: zsm
# Date: 2018/9/11 12:09
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_skc_country_add_order 
        select b.product_code
            , b.color_code
            , a.order_date as add_order_date
            , sum(a.add_order_qty) as add_order_qty
            , sum(a.arrived_qty) as arrived_qty
            , sum(a.not_arrived_qty) as not_arrived_qty
            , current_timestamp as etl_time
        from {p_edw_schema}.mid_sku_add_order_info a 
        inner join {p_edw_schema}.dim_product_sku b 
            on a.sku_id = b.sku_id
        group by 
            b.product_code
            , b.color_code
            , a.order_date
    '''
    spark.execute_sql(sql_insert)
