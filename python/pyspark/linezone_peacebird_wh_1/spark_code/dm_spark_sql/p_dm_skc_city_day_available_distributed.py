# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_skc_city_day_available_distributed
# Author: zsm
# Date: 2018/9/10 13:29
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql = '''
        insert overwrite table {p_dm_schema}.dm_skc_city_day_available_distributed partition(day_date)
        select a.product_code
            , a.color_code
            , b.city_code as city_code
            , max(b.city_long_code) as city_long_code
            , count(1) as available_distributed_store_count
            , sum(case when a.available_is_fullsize='N' then 1 else 0 end) as available_brokensize_store_count
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_skc_store_day_available_fullsize a
        inner join {p_edw_schema}.dim_store b 
            on a.store_code = b.store_code
        where 
            day_date = '{p_input_date}'
        group by 
            a.product_code
            , a.color_code
            , b.city_code
    '''
    spark.execute_sql(sql)
