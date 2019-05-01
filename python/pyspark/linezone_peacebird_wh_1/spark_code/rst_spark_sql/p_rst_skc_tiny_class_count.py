# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_skc_tiny_class_count
# Author: zsm
# Date: 2018/9/12 17:27
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_tmp_count = '''
        select 
            year,
            quarter,
            tiny_class,
            pre_class,
            count(1) as counts,
            day_date
        from {p_rst_schema}.rst_skc_tiny_class_pre_classify
        where day_date = '{p_input_date}'   --取今天的数据
        group by day_date,year,quarter,tiny_class,pre_class
    '''

    sql_tmp_join = '''
        with tmp1 as (
            select 
                year,
                quarter,
                tiny_class,
                day_date
            from tmp_count
        group by day_date,year,quarter,tiny_class
        ),
        tmp2 as (
            select distinct pre_class
            from tmp_count
        )
        select 
            year,
            quarter,
            tiny_class,
            day_date,
            pre_class
        from tmp1 
        inner join tmp2 on 1=1
    '''

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_skc_tiny_class_count partition(day_date)
        select 
            a.year,
            a.quarter,
            a.tiny_class,
            a.pre_class,
            coalesce(b.counts, 0) as counts,
            current_timestamp as etl_time,
            a.day_date
        from tmp_join a 
        left join tmp_count b 
            on a.year = b.year and a.quarter = b.quarter 
            and a.tiny_class = b.tiny_class 
            and a.day_date = b.day_date
            and a.pre_class = b.pre_class
    '''
    spark.create_temp_table(sql_tmp_count, 'tmp_count')
    print('1')
    spark.create_temp_table(sql_tmp_join, 'tmp_join')
    print('2')
    spark.execute_sql(sql_insert)
    print('3')
    spark.drop_temp_table('tmp_count')
    spark.drop_temp_table('tmp_join')
