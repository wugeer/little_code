# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_store_day_brokensize_rate
# Author: zsm
# Date: 2018/9/10 14:22
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql = '''
        insert overwrite table {p_dm_schema}.dm_store_day_brokensize_rate partition(day_date)
        select
            store_code
            , sum(case when is_fullsize = 'N' then 1 else 0 end) / count(1) as brokensize_rate
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_skc_store_day_fullsize
        where 
            day_date='{p_input_date}'
        group by 
            store_code
    '''
    spark.execute_sql(sql)