# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_skc_country_day_brokensize_rate
   Description :
   Author :       yangming
   date：          2018/9/10
-------------------------------------------------
   Change Activity:
                   2018/9/10:
-------------------------------------------------
"""
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    foo = SparkInit(file_name)

    sql = """
        insert overwrite table {p_dm_schema}.dm_skc_country_day_brokensize_rate
        partition(day_date)
        select a.product_code
            , a.color_code
            , (sum(case when available_is_fullsize = 'N' then 1 else 0 end)/ count(1)) as brokensize_rate  -- 断码率
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_skc_store_day_available_fullsize a
        inner join (select * from {p_edw_schema}.dim_store where is_store='Y' and status='正常') b 
            on a.store_code = b.store_code
        where a.day_date='{p_input_date}'
        group by a.product_code, a.color_code   
    """

    foo.execute_sql(sql)
