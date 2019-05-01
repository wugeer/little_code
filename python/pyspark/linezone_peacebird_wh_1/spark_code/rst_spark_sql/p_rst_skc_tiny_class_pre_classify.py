# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_tiny_class_country_day_pre_classify
# Author: zsm
# Date: 2018/9/12 16:39
# ----------------------------------------------------------------------------
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_tmp_mod_skc_expected_class_basis = '''
        with tmp1 as (
            select 
                class_celling_slope
                , class_floor_slope
                , etl_time
                , quarter_id
                , year_id
                , row_number() over(partition by year_id, quarter_id order by etl_time desc) as new_index --按年份季节分组，更新时间倒序排列
            from {p_edw_schema}.mod_skc_expected_class_basis
        )            
        select 
            class_celling_slope
            , class_floor_slope 
            , etl_time 
            , quarter_id
            , year_id
        from tmp1
        where new_index = 1 --取最新记录
    '''
    spark.create_temp_table(sql_tmp_mod_skc_expected_class_basis, 'tmp_mod_skc_expected_class_basis')

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_skc_tiny_class_pre_classify partition(day_date)
        select
             b.year_id as year
            , b.quarter_id as quarter
            , b.tiny_class
            , a.product_code
            , d.product_name
            , a.color_code
            , d.color_name
            , a.distributed_days
            , a.sales_out_rate   -- 售罄率
            , a.pre_class
            , c.class_celling_slope
            , c.class_floor_slope
            , current_timestamp as etl_time 
            , a.day_date as day_date
        from {p_dm_schema}.dm_skc_country_day_pre_classify a
        inner join {p_edw_schema}.dim_product b   -- 取skc所属的年份、季节、小类
            on a.product_code = b.product_code
        inner join tmp_mod_skc_expected_class_basis c  -- 取预期分类斜率的上限和下限
            on b.year_id = c.year_id and b.quarter_id = c.quarter_id
        inner join {p_edw_schema}.dim_product_skc d         		--取商品名称，颜色名称
        on a.product_code = d.product_code and a.color_code = d.color_code
        where a.day_date = '{p_input_date}'
    '''
    spark.execute_sql(sql_insert)
