# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_store_day_sell_well_skc
# Author: zsm
# Date: 2018/9/10 14:37
# ----------------------------------------------------------------------------


import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_sell_well_skc = '''
        
        select store_code
            , product_code
            , color_code
            , sum(last_fourteen_days_sales_amt) over (partition by store_code order by last_fourteen_days_sales_amt desc) as part_amt
            , sum(last_fourteen_days_sales_amt) over (partition by store_code) as total_amt
            , rank() over (partition by store_code order by last_fourteen_days_sales_amt desc) as order_num
        from {p_dm_schema}.dm_skc_store_day_sales 
        where 
            day_date = '{p_input_date}'
    '''

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_store_day_sell_well_skc PARTITION(day_date)
        select store_code
            , product_code
            , color_code
            , order_num
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from sell_well_skc
        where
            part_amt - total_amt * {p_sell_well_rate} <= 0
    '''
    spark.create_temp_table(sql_sell_well_skc, 'sell_well_skc')
    spark.execute_sql(sql_insert)
    spark.drop_temp_table('sell_well_skc')
