# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_rst_skc_city_brokensize_count
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

    # 创建tmp1
    sql_tmp1 = """
        select a.product_code
            , a.color_code
            , sum(case a.available_is_fullsize when 'N' then 1 else 0 end) as before_rep_allot_brokensize_count
            , count(1) as before_rep_allot_skc_count
            , c.org_longcode
            , '{p_input_date}' as day_date
            , current_timestamp
        from {p_dm_schema}.dm_skc_store_day_available_fullsize a
        inner join {p_edw_schema}.dim_stockorg b on a.store_code = b.org_code              --门店
        inner join {p_edw_schema}.dim_stockorg c on b.parent_id = c.org_id     --城市
        where a.day_date = '{p_input_date}'
        group by a.product_code,color_code,c.org_longcode
    """

    # 创建tmp11
    sql_tmp11 = """
        select a.product_code
            , a.color_code
            , sum(case a.is_fullsize when 'N' then 1 else 0 end) as after_rep_allot_brokensize_count
            , count(1) as after_rep_allot_skc_count
            , c.org_longcode
            , '{p_input_date}' as day_date
            , current_timestamp
        from {p_dm_schema}.dm_skc_store_day_after_replenish_allot_fullsize a
        inner join {p_edw_schema}.dim_stockorg b on a.store_code = b.org_code              --门店
        inner join {p_edw_schema}.dim_stockorg c on b.parent_id = c.org_id     --城市
        where a.day_date = '{p_input_date}'
        group by a.product_code,color_code,c.org_longcode
    """

    sql = """
        insert overwrite table {p_rst_schema}.rst_skc_city_brokensize_count
        partition(day_date)
        select b.org_longcode as city_long_code
            , b.product_code
            , p.product_name
            , b.color_code
            , p.color_name
            , a.before_rep_allot_skc_count
            , a.before_rep_allot_brokensize_count
            , b.after_rep_allot_skc_count
            , b.after_rep_allot_brokensize_count
            , current_timestamp as etl_time
            , b.day_date
        from tmp11 b 
        inner join {p_edw_schema}.dim_product_skc p on b.product_code = p.product_code and b.color_code = p.color_code
        left join tmp1 a on a.org_longcode = b.org_longcode and a.product_code = b.product_code 
            and a.color_code = b.color_code
    """

    tmp1 = foo.create_temp_table(sql_tmp1, 'tmp1')
    tmp11 = foo.create_temp_table(sql_tmp11, 'tmp11')

    foo.execute_sql(sql)

    foo.drop_temp_table('tmp1')
    foo.drop_temp_table('tmp11')