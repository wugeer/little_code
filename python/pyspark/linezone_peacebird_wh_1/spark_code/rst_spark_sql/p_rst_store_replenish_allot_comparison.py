# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_store_replenish_allot_comparison
# Author: zsm
# Date: 2018/9/12 17:53
# ----------------------------------------------------------------------------
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_tmp1 = '''
        select
            a.store_code
            , store.city_long_code
            , store.zone_id as zone_long_code
            , store.dq_long_code
            , a.year as year
            , a.quarter as quarter
            , null as big_class
            , null as tiny_class
            , null as mictiny_class
            , sum(a.last_seven_days_sales_qty) as last_seven_days_sales_qty
            , sum(a.replenish_qty) as replenish_qty
            , sum(a.receive_qty) as receive_qty
            , sum(a.send_qty) as send_qty
            , sum(a.available_sell_well_skc_count) as available_sell_well_skc_count
            , sum(a.available_all_skc_count) as available_all_skc_count
            , sum(a.available_sell_well_stock_qty) as available_sell_well_stock_qty
            , sum(a.available_all_skc_stock_qty) as available_all_skc_stock_qty
            , sum(a.available_sell_well_brokensize_numerator) / sum(a.available_sell_well_brokensize_denominator) as available_sell_well_brokensize_rate
            , sum(a.available_all_brokensize_numerator) / sum(a.available_all_brokensize_denominator) as available_all_brokensize_rate
            , sum(a.after_sell_well_skc_count) as after_sell_well_skc_count
            , sum(a.after_all_skc_count) as after_all_skc_count
            , sum(a.after_sell_well_stock_qty) as after_sell_well_stock_qty
            , sum(a.after_all_skc_stock_qty) as after_all_skc_stock_qty
            , sum(a.after_sell_well_brokensize_numerator) / sum(a.after_sell_well_brokensize_denominator) as after_sell_well_brokensize_rate
            , sum(a.after_all_brokensize_numerator) / sum(a.after_all_brokensize_denominator) as after_all_brokensize_rate  
        from {p_dm_schema}.dm_store_mictiny_class_replenish_allot_comparison as a
        inner join {p_edw_schema}.dim_store as store
            on a.store_code = store.store_code 
        where 
            a.day_date = '{p_input_date}'
        group by 
            a.store_code,store.city_long_code,store.zone_id,store.dq_long_code,a.year,a.quarter
    '''

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_store_replenish_allot_comparison partition(day_date)
        select 
            tmp1.*
            , rank() over(order by tmp1.dq_long_code,tmp1.zone_long_code,tmp1.city_long_code,tmp1.store_code) as order_num
            , CURRENT_TIMESTAMP as etl_time
            , '{p_input_date}' as day_date
        from tmp1
    '''

    spark.create_temp_table(sql_tmp1, 'tmp1')
    spark.execute_sql(sql_insert)
    spark.drop_temp_table('tmp1')
