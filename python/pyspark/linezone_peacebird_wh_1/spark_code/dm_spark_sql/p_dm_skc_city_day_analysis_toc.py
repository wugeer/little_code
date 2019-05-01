# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_skc_city_day_analysis_toc
# Author: zsm
# Date: 2018/9/10 11:06
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit
from utils.tools import TaskThreadPoolExecutors


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_sku_stock = '''
        select a.product_code
            , a.color_code
            , a.store_code
            , sum(case when a.available_stock_qty < 0 then 0 else a.available_stock_qty end) as available_stock_qty
        from {p_dm_schema}.dm_sku_store_day_available_stock as a
        inner join {p_edw_schema}.dim_store as store
            on a.store_code = store.store_code 
        where 
            a.day_date = '{p_input_date}'
            and store.status = '正常'
        group by a.store_code
            , a.product_code
            , a.color_code
    '''

    sql_stock = '''
        select product_code
          , color_code
          , store_code
          , (case when available_stock_qty > 0 then 'Y' else 'N' end) as has_available_stock
        from sku_stock
    '''

    sql_sales = '''
        select product_code
            , color_code
            , store_code 
            , (case when last_fourteen_days_sales_qty > 0 then 'Y' else 'N' end) as has_his_sales
        from {p_dm_schema}.dm_skc_store_day_sales
        where day_date = '{p_input_date}'
    '''

    sql_fullsize = '''
        select product_code
            , color_code
            , store_code 
            , is_fullsize
        from {p_dm_schema}.dm_skc_store_day_fullsize
        where day_date = '{p_input_date}'
    '''

    sql_available_fullsize = '''
        select product_code
            , color_code
            , store_code 
            , available_is_fullsize
        from {p_dm_schema}.dm_skc_store_day_available_fullsize
        where day_date = '{p_input_date}'
    '''

    sql_mix = '''
        select a.product_code
            , a.color_code
            , a.store_code
            , coalesce(a.has_available_stock,'N') as has_available_stock
            , coalesce(c.has_his_sales,'N') as has_his_sales
            , e.is_fullsize as is_fullsize--没有判断是否断码的为null
            , f.available_is_fullsize as available_is_fullsize--没有判断是否断码的为null
        from stock a 
        left join sales c 
            on a.product_code=c.product_code 
            and a.color_code=c.color_code 
            and a.store_code=c.store_code 
        left join fullsize e 
            on a.product_code=e.product_code 
            and a.color_code=e.color_code 
            and a.store_code=e.store_code 
        left join available_fullsize f 
            on a.product_code=f.product_code 
            and a.color_code=f.color_code 
            and a.store_code=f.store_code 
    '''

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_skc_city_day_analysis_toc partition(day_date)
        select a.product_code
            , a.color_code
            , max(b.city_code) as city_code
            , b.city_long_code as city_long_code
            , b.is_toc as is_toc
            , count(case when a.has_his_sales = 'N' and a.has_available_stock = 'N' and c.store_code is not null
                                then a.store_code end) as no_sales_no_available_stock_store_count--无销量，无库存（加在途）且曾经铺过货
            , count(case when a.has_his_sales = 'Y' and a.has_available_stock = 'N'
                                then a.store_code end) as has_sales_no_available_stock_store_count--有销量，无库存（加在途）
            , count(case when a.has_his_sales = 'N' and a.available_is_fullsize = 'Y'
                                then a.store_code end) as no_sales_available_fullsize_store_count--无销量。齐码（加在途）
            , count(case when a.has_his_sales = 'N' and a.available_is_fullsize = 'N'
                                then a.store_code end) as no_sales_available_brokensize_store_count--无销量，断码（加在途）
            , count(case when a.has_his_sales = 'Y' and a.available_is_fullsize = 'N'
                                then a.store_code end) as has_sales_available_brokensize_store_count--有销量，断码（加在途）
            , count(case when a.has_his_sales = 'Y' and a.available_is_fullsize = 'Y'
                                then a.store_code end) as has_sales_available_fullsize_store_count--有销量，齐码（加在途）
            , count(case when a.has_his_sales = 'Y' and a.is_fullsize = 'Y'
                                then a.store_code end) as has_sales_fullsize_store_count--有销量，齐码（无在途）--以上七个用于七色图指标
            , count(case when a.has_his_sales = 'N' and a.available_is_fullsize is not null
                                then a.store_code end) as no_sales_has_available_stock_store_count--无销量有库存门店数(加在途) 用于无销售门店比例分子
            , count(case when a.has_available_stock = 'Y'
                                then a.store_code end) as has_available_stock_store_count --加在途后有库存门店数（用于无销售门店比例分母和销售断码率分母）
            , count(case when a.is_fullsize is not null
                                then a.store_code end) as has_stock_store_count --有库存门店数（无在途)
            , count(case when a.available_is_fullsize = 'N'
                                then a.store_code end) as available_brokensize_store_count --断码门店数（加在途）
            , count(case when a.is_fullsize = 'N'
                                then a.store_code end) as brokensize_store_count --断码门店数（无在途）    
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from mix a 
        inner join {p_edw_schema}.dim_store b 
            on a.store_code = b.store_code 
        left join dm.dm_skc_store_day_distributed_info as c
            on a.product_code = c.product_code
            and a.color_code = c.color_code
            and a.store_code = c.store_code
        group by 
            a.product_code
            , a.color_code
            , b.city_long_code
            , b.is_toc
    '''

    spark.create_temp_table(sql_sku_stock, 'sku_stock')
    # 并行生成所需dm层的基础表
    sql_list = [sql_stock, sql_sales, sql_fullsize, sql_available_fullsize]
    table_name_list = ['stock', 'sales', 'fullsize', 'available_fullsize']
    multi = TaskThreadPoolExecutors(spark.create_temp_table, 20, sql_list, table_name_list)
    # 目的是等待子线程结束再向下执行
    multi.result

    spark.create_temp_table(sql_mix, 'mix')
    spark.execute_sql(sql_insert)
