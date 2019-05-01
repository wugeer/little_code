# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_rst_city_brokensize_count
   Description :
   Author :       yangming
   date：          2018/9/12
-------------------------------------------------
   Change Activity:
                   2018/9/12:
-------------------------------------------------
"""
# TODO dm_skc_store_day_after_replenish_allot_fullsize是空表，需调试

import os

from utils.tools import SparkInit

if __name__ == '__main__':
    # app_name
    file_name = os.path.basename(__file__)

    # 初始化spark环境
    foo = SparkInit(file_name)

    # 创建tmp2
    sql_tmp2 = """
        select sum(case a.available_is_fullsize when 'N' then 1 else 0 end) as before_rep_allot_all_brokensize_count
            , count(a.available_is_fullsize) as before_rep_allot_all_skc_count
            , c.org_longcode
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_skc_store_day_available_fullsize a
        inner join {p_edw_schema}.dim_stockorg b on a.store_code = b.org_code              --门店
        inner join {p_edw_schema}.dim_stockorg c on b.parent_id= c.org_id     --城市
        where a.day_date = '{p_input_date}'
        group by c.org_longcode
    """

    # 创建tmp22
    sql_tmp22 = """
        select sum(case a.is_fullsize when 'N' then 1 else 0 end) as after_rep_allot_all_brokensize_count
            , count(a.is_fullsize) as after_rep_allot_all_skc_count
            , c.org_longcode
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_skc_store_day_after_replenish_allot_fullsize a
        inner join {p_edw_schema}.dim_stockorg b on a.store_code = b.org_code              --门店
        inner join {p_edw_schema}.dim_stockorg c on b.parent_id = c.org_id     --城市
        where a.day_date = '{p_input_date}'
        group by c.org_longcode
    """

    sql = """
        insert overwrite table {p_rst_schema}.rst_city_brokensize_count
        partition(day_date)
        select b.org_longcode as city_long_code
            , a.before_rep_allot_all_skc_count as before_rep_allot_skc_count
            , a.before_rep_allot_all_brokensize_count as before_rep_allot_brokensize_count 
            , b.after_rep_allot_all_skc_count as after_rep_allot_skc_count
            , b.after_rep_allot_all_brokensize_count as after_rep_allot_brokensize_count
            , current_timestamp as etl_time
            , b.day_date
        from tmp22 b 
        left join tmp2 a on a.org_longcode = b.org_longcode
    """

    tmp2 = foo.create_temp_table(sql_tmp2, 'tmp2')
    tmp22 = foo.create_temp_table(sql_tmp22, 'tmp22')

    foo.execute_sql(sql)

    foo.drop_temp_table('tmp2')
    foo.drop_temp_table('tmp22')