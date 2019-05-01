# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_skc_country_add_order_expected_arrival
# Author: zsm
# Date: 2018/9/11 12:11
# ----------------------------------------------------------------------------
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_skc_country_add_order_expected_arrival
        select b.product_code
            , b.color_code
            , a.expected_arrive_date as expected_arrival_date
            , sum(a.expected_arrive_qty) as expected_arrival_qty
            , current_timestamp as etl_time
        from {p_edw_schema}.mid_sku_add_order_expected_arrival a 
        inner join {p_edw_schema}.dim_product_sku b 
            on a.sku_id = b.sku_id
        group by 
            b.product_code
            , b.color_code
            , a.expected_arrive_date
    '''
    spark.execute_sql(sql_insert)
