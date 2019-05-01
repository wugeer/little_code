# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_skc_brokensize_has_stock_store_count_assess
# Author: lh
# Date: 2019年1月15日10:58:24
# ----------------------------------------------------------------------------
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)
  
    # skc 加在途有库存门店数
    sql_tmp_skc_has_stock_store_count = '''
        select
            a.product_code
            , a.color_code
            , sum(available_distributed_store_count) as has_stock_store_count
        from {p_dm_schema}.dm_skc_city_day_available_distributed a
        where day_date = '{p_input_date}'
        group by a.product_code,a.color_code
    '''

    # 取传入日期当天的 skc断码率
    sql_tmp_skc_brokensize_rate = '''
        select
            a.product_code
            , a.color_code
            , a.brokensize_rate
        from {p_dm_schema}.dm_skc_country_day_brokensize_rate a
        where day_date = '{p_input_date}'
    '''

    # skc 断码率 和 有库存门店数的 组合 full 没有 为null
    sql_tmp_skc_all = '''
        select
            coalesce(a.product_code, g.product_code) as product_code
            , coalesce(a.color_code, g.color_code) as color_code
            , a.has_stock_store_count
            , g.brokensize_rate
        from tmp_skc_has_stock_store_count a 
        full join tmp_skc_brokensize_rate g  --取断码率
            on a.product_code = g.product_code 
            and a.color_code = g.color_code
     '''
 
    # 将数据 插入到 目标中
    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_skc_brokensize_has_stock_store_count_assess
        select
            dps.product_id
            , dps.product_code
            , dps.color_id
            , dps.color_code
            , a.brokensize_rate
            , a.has_stock_store_count
            , '{p_input_date}' as day_date
            , current_timestamp as etl_time
        from tmp_skc_all a
        inner join {p_edw_schema}.dim_product_skc dps        		--取skc product_id color_id
            on a.product_code = dps.product_code 
            and a.color_code = dps.color_code

        union all

        select * 
        from {p_rst_schema}.rst_skc_brokensize_has_stock_store_count_assess
        where day_date <> '{p_input_date}'
    '''

    # 创建临时表
    spark.create_temp_table(sql_tmp_skc_has_stock_store_count, 'tmp_skc_has_stock_store_count')
    spark.create_temp_table(sql_tmp_skc_brokensize_rate, 'tmp_skc_brokensize_rate')
    spark.create_temp_table(sql_tmp_skc_all, 'tmp_skc_all')

    spark.execute_sql(sql_insert)

    # 删除临时表
    spark.drop_temp_table('tmp_skc_has_stock_store_count')
    spark.drop_temp_table('tmp_skc_brokensize_rate')
    spark.drop_temp_table('tmp_skc_all')
