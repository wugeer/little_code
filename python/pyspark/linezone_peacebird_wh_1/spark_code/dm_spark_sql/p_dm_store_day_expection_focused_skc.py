# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_store_day_expection_focused_skc
   Description :
   Author :       yangming
   date：          2018/9/7
-------------------------------------------------
   Change Activity: 计算门店每天重点关注skc，按照销量排名
                   2018/9/7:
-------------------------------------------------
"""
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    # app_name
    file_name = os.path.basename(__file__)

    # 初始化spark环境
    foo = SparkInit(file_name)

    # 获取单店中 skc 的销售额
    sql_sales = """          
        select 
            a.store_code
            , a.product_code
            , a.color_code
            , a.last_fourteen_days_sales_qty as sales_qty
            , a.last_fourteen_days_sales_amt as sales_amt
        from {p_dm_schema}.dm_skc_store_day_sales a
        where a.day_date = '{p_input_date}'
    """

    sales = foo.create_temp_table(sql_sales, 'sales')

    sql_insert = """
        insert overwrite table {p_dm_schema}.dm_store_day_expection_focused_skc
        partition(day_date)
        select
            skc.product_code
            , skc.color_code
            , skc.store_code
            , exp.store_expection
            , fullsize.is_fullsize
            , sales.sales_qty as last_two_week_sales_qty
            , current_timestamp as etl_time
             , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_store_day_sell_well_skc as skc
        inner join sales
            on sales.store_code = skc.store_code
            and sales.product_code = skc.product_code
            and sales.color_code = skc.color_code
        inner join (select * from {p_dm_schema}.dm_store_day_expection where day_date = '{p_input_date}') as exp
            on skc.store_code = exp.store_code
        inner join (select * from {p_dm_schema}.dm_skc_store_day_fullsize where day_date = '{p_input_date}') as fullsize
            on skc.store_code = fullsize.store_code
            and skc.product_code = fullsize.product_code
            and skc.color_code = fullsize.color_code
        where
            skc.day_date = '{p_input_date}'
            and (exp.store_expection = 1 or exp.store_expection = -1)
    """

    foo.execute_sql(sql_insert)

    foo.drop_temp_table('sales')
