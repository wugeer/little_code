# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_store_day_sell_well_brokensize_rate
# Author: zsm
# Date: 2018/9/10 14:33
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql = '''
        insert overwrite table {p_dm_schema}.dm_store_day_sell_well_brokensize_rate partition(day_date)
        select a.store_code
            , count(case when a.is_fullsize = 'N' then a.store_code end) / count(1) as sell_well_brokensize_rate
            , CURRENT_TIMESTAMP as etl_time
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_skc_store_day_fullsize a
        inner join {p_dm_schema}.dm_store_day_sell_well_skc as b
            on b.product_code = a.product_code
            and b.color_code = a.color_code
            and b.store_code = a.store_code
            and b.day_date = a.day_date
        where 
            a.day_date = '{p_input_date}'
        group by 
            a.store_code
    '''
    spark.execute_sql(sql)