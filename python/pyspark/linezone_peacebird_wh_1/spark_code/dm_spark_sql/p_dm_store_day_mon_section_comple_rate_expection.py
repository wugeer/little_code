# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_store_day_mon_section_comple_rate_expection.py
   Description :
   Author :       zsm
   date：          2018/8/28
-------------------------------------------------
   Change Activity:
                   2018/8/28:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_dm_store_day_mon_section_comple_rate_expection.py
-- 源表: 
-- 目标表:{p_dm_schema}.dm_store_day_mon_section_comple_rate；{p_dm_schema}.dm_store_day_expection
-- 程序描述:
-- 程序路径: /opt/peacebird/dm_spark_sql/p_dm_store_day_mon_section_comple_rate_expection.py
-- 程序备注:  
-- 版本号信息: v1.0 create
--             v2.0 alter   改写pyspark
"""

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_numerator = '''
        select 
            b.org_code as store_code
            ,coalesce(a.fct_mon_section_sales_amt,0) as fct_mon_section_sales_amt
            ,b.target_mon_section_sales_amt
        from {p_dm_schema}.dm_org_day_mon_section_fct_sales_amt a
        right join {p_dm_schema}.dm_org_day_mon_section_target_sales_amt b
            on a.org_code = b.org_code
            and a.day_date = b.day_date
        where 
            b.day_date = '{p_input_date}'
            and b.org_type = '门店'
    '''
    spark.create_temp_table(sql_numerator, 'numerator')
    numerator = spark.spark.table('numerator')

    # 判断numerator是否为空
    # numerator为空表示没有给到今天得目标营业额，取出最接近那天的完成率和门店分类数据作为今天得数据插入
    # print(len(numerator.head(1))) #空的话为0
    # print(numerator.count()) #空的话为0
    if numerator.rdd.isEmpty():
        # 由于不允许从一个表中查出数据再插入同一张表，因此需要先将查出来的数据放到临时表
        sql_comple_rate_tmp = '''
            select 
                a.store_code                                  
                , a.store_expection                           
                , a.act_comple_rate
                , a.pla_comple_rate
                , current_timestamp as etl_time
                , '{p_input_date}' as day_date
            from {p_dm_schema}.dm_store_day_mon_section_comple_rate as a
            inner join (
                select max(day_date) as day_date
                from {p_dm_schema}.dm_store_day_mon_section_comple_rate 
                where 
                    month(day_date) = month('{p_input_date}')
                ) as b
                on a.day_date = b.day_date
        '''
        spark.create_temp_table(sql_comple_rate_tmp, 'comple_rate_tmp')
        sql_insert_comple_rate = '''
            insert overwrite table {p_dm_schema}.dm_store_day_mon_section_comple_rate partition(day_date)
            select * from comple_rate_tmp
        '''
        spark.execute_sql(sql_insert_comple_rate)

        sql_store_expection_tmp = '''
            select  
                store_code
                , store_expection
                , CURRENT_TIMESTAMP as etl_time
                , '{p_input_date}' as day_date
            from {p_dm_schema}.dm_store_day_expection as a
            inner join (
                select max(day_date) as day_date
                from {p_dm_schema}.dm_store_day_expection 
                where 
                    month(day_date) = month('{p_input_date}')
                ) as b
                on a.day_date = b.day_date
        '''
        spark.create_temp_table(sql_store_expection_tmp, 'store_expection_tmp')
        sql_insert_store_expection = '''
            insert overwrite table {p_dm_schema}.dm_store_day_expection partition(day_date)
            select * from store_expection_tmp
        '''
        spark.execute_sql(sql_insert_store_expection)

    # 不为空则插入今天得数据
    else:
        # 本月一号至今天累计实际营业额/本月一号至今天累计目标营业额，门店分类
        sql_insert_store_expection = '''
            insert overwrite table {p_dm_schema}.dm_store_day_expection partition(day_date)
            select 
                store_code
                , (case 
                    when coalesce(fct_mon_section_sales_amt,0) / target_mon_section_sales_amt > 0.9
                        then 1 
                    when coalesce(fct_mon_section_sales_amt,0) / target_mon_section_sales_amt < 0.8 
                        then -1 
                    else 0
                end) as store_expection
            , CURRENT_TIMESTAMP as etl_time
            , '{p_input_date}' as day_date
            from numerator
        '''
        spark.execute_sql(sql_insert_store_expection)
        # 将本月给出的最大天数那天的目标营业额作为完成率分母
        sql_denominator = """
            select 
                org_code as store_code
                , max(day_date) AS day_date
                , max(target_mon_section_sales_amt) AS target_mon_section_sales_amt
            from {p_dm_schema}.dm_org_day_mon_section_target_sales_amt 
            where
                month(day_date) = month('{p_input_date}')
                and org_type='门店'
            GROUP BY org_code
        """
        spark.create_temp_table(sql_denominator, "denominator")
        print(spark.spark.table("denominator"))
        sql_insert_comple_rate = """
            insert overwrite table {p_dm_schema}.dm_store_day_mon_section_comple_rate partition(day_date)
            select
                d.store_code
                , exp.store_expection
                , n.fct_mon_section_sales_amt / d.target_mon_section_sales_amt as act_comple_rate
                , n.target_mon_section_sales_amt / d.target_mon_section_sales_amt as pla_comple_rate
                , CURRENT_TIMESTAMP as etl_time
                , '{p_input_date}' as day_date
            from denominator as d
            inner join numerator as n
                on d.store_code=n.store_code
            inner join {p_dm_schema}.dm_store_day_expection as exp
                on d.store_code=exp.store_code
            where 
                exp.day_date = '{p_input_date}'
                and (exp.store_expection = 1 or exp.store_expection = -1)
            """
        spark.execute_sql(sql_insert_comple_rate)
