# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_store_mictiny_class_replenish_allot_comparison
# Author: zsm
# Date: 2018/9/12 17:40
# ----------------------------------------------------------------------------
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_store_mictiny_class_replenish_allot_comparison partition(day_date)
        select
            a.store_code
            , store.city_long_code    
            , store.zone_id as zone_long_code
            , store.dq_long_code
            , a.year
            , a.quarter
            , a.big_class
            , a.tiny_class
            , a.mictiny_class
            , a.last_seven_days_sales_qty
            , a.replenish_qty
            , a.receive_qty
            , a.send_qty
            , a.available_sell_well_skc_count
            , a.available_all_skc_count
            , a.available_sell_well_stock_qty
            , a.available_all_skc_stock_qty
            , a.available_sell_well_brokensize_numerator / a.available_sell_well_brokensize_denominator as available_sell_well_brokensize_rate
            , a.available_all_brokensize_numerator / a.available_all_brokensize_denominator as available_all_brokensize_rate
            , a.after_sell_well_skc_count
            , a.after_all_skc_count
            , a.after_sell_well_stock_qty
            , a.after_all_skc_stock_qty
            , a.after_sell_well_brokensize_numerator / a.after_sell_well_brokensize_denominator as after_sell_well_brokensize_rate
            , a.after_all_brokensize_numerator / a.after_all_brokensize_denominator as after_all_brokensize_rate
            , rank() over(order by store.dq_long_code,store.zone_id,store.city_long_code,store.store_code) as order_num
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_store_mictiny_class_replenish_allot_comparison as a
        inner join {p_edw_schema}.dim_store as store
            on a.store_code = store.store_code 
        where 
            day_date = '{p_input_date}'
    '''
    spark.execute_sql(sql_insert)
