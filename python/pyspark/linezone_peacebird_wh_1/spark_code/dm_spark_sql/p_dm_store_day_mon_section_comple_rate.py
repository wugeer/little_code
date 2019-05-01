# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_store_day_mon_section_comple_rate
# Author: zsm
# Date: 2018/9/13 17:56
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_numerator = '''
        select 
            b.org_code as store_code
            , coalesce(a.fct_mon_section_sales_amt,0) as fct_mon_section_sales_amt
            , b.target_mon_section_sales_amt
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

    # 不为空则插入今天得数据
    else:
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

