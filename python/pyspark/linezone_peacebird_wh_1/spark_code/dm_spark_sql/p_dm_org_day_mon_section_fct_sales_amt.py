# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_org_day_mon_section_fct_sales_amt
# Author: zsm
# Date: 2018/9/6 10:26
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit
from utils.tools import TaskThreadPoolExecutors


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    fct = SparkInit(file_name)

    sql_store_sales = '''
        select 
            min(b.org_id) as org_id
            , a.store_code as org_code
            , min(b.org_longcode) as org_long_code
            , '门店' as org_type
            , sum(a.sales_amt) as sales_amt
            , '{p_input_date}' as day_date
        from {p_edw_schema}.mid_skc_store_day_sales a
        inner join {p_edw_schema}.dim_stockorg b
            on a.store_code = b.org_code
        where
            a.day_date <= '{p_input_date}'
            and a.day_date >= trunc('{p_input_date}','MM')
        group by
            a.store_code
    '''

    sql_city_sales = '''
        select 
            b.city_id as org_id
            , min(b.city_code) as org_code
            , min(b.city_long_code) as org_long_code
            , '城市' as org_type
            , sum(a.sales_amt) as sales_amt
            , a.day_date
        from store_sales a
        inner join {p_edw_schema}.dim_store b
            on a.org_code = b.store_code
        group by
            b.city_id
            , a.day_date
    '''

    sql_zone_sales = '''
        select
          b.zone_id as org_id
          , b.zone_id as org_code
          , b.zone_id as org_long_code
          , '区域' as org_type
          , sum(a.sales_amt) as sales_amt
          , a.day_date
        from store_sales a
        inner join {p_edw_schema}.dim_store b
            on a.org_code = b.store_code
        group by
            b.zone_id
            , a.day_date
    '''

    sql_region_sales = '''
        select 
          b.dq_id as org_id
          , min(b.dq_code) as org_code
          , min(b.dq_long_code) as org_long_code
          , '大区' as org_type
          , sum(a.sales_amt) as sales_amt
          , a.day_date
        from store_sales a
        inner join {p_edw_schema}.dim_store b
            on a.org_code=b.store_code
        group by
            b.dq_id
            , a.day_date
    '''

    sql_country_sales = '''
        select 
            b.nanz_id as org_id
            , min(b.nanz_code) as org_code
            , min(b.nanz_code) as org_long_code
            , '男装' as org_type
            , sum(a.sales_amt) as sales_amt
            , a.day_date
        from store_sales a
        inner join {p_edw_schema}.dim_store b
            on a.org_code = b.store_code
        group by
            b.nanz_id
            , a.day_date
    '''

    # 生成门店基础表
    fct.create_temp_table(sql_store_sales, 'store_sales')
    # 多线程聚合各个维度
    sql_name_list = [sql_city_sales, sql_zone_sales, sql_region_sales, sql_country_sales]
    multi = TaskThreadPoolExecutors(fct.return_df, 20, sql_name_list)

    # 获取多线程返回的城市，区域，大区，全国聚合结果
    sales = None
    for sale in multi.result:
        if sales is None:
            sales = sale
        else:
            sales = sales.unionAll(sale)
    sales.unionAll(fct.spark.table("store_sales"))

    # 注册一个临时表,插入数据
    sales.registerTempTable("sales")
    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_org_day_mon_section_fct_sales_amt partition(day_date)
        select
            org_code
            , org_long_code
            , org_type
            , sales_amt as fct_mon_section_sales_amt
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from sales
    '''
    fct.execute_sql(sql_insert)

    # drop临时表
    fct.drop_temp_table("store_sales")