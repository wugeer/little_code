# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_store_day_expection
# Author: zsm
# Date: 2018/9/13 17:41
# ----------------------------------------------------------------------------
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_sales_comp = '''
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
    spark.create_temp_table(sql_sales_comp, 'sales_comp')
    sales_comp = spark.spark.table('sales_comp')

    # 判断sales_comp是否为空
    # sales_comp为空表示没有给到今天得目标营业额，取出最接近那天的完成率和门店分类数据作为今天得数据插入
    # print(len(sales_comp.head(1))) #空的话为0
    # print(sales_comp.count()) #空的话为0
    if sales_comp.rdd.isEmpty():
        # 由于不允许从一个表中查出数据再插入同一张表，因此需要先将查出来的数据放到临时表
        sql_store_expection_tmp = '''
            select  
                a.store_code
                , a.store_expection
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
            from sales_comp
            '''
        spark.execute_sql(sql_insert_store_expection)
