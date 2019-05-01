# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_skc_country_day_life_cycle
   Description :
   Author :       yangming
   date：          2018/9/10
-------------------------------------------------
   Change Activity:
                   2018/9/10:
-------------------------------------------------
"""
import os
import datetime
from datetime import timedelta
from dateutil.parser import parse
from utils.tools import SparkInit


# TODO : 怎样更好替代掉之前的udf
def date_trunc(interval, date_str):
    """
    截断到指定精度，返回相应的日期字符串
    :param interval: ['week', 'month', 'year']
    :param date_str:
    :return: after_trunc_date_str
    """
    date_obj = parse(date_str)
    # date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    if interval == 'week':
        res = date_obj - timedelta(days=(date_obj.isocalendar()[2] - 1))
    elif interval == 'month':
        res = datetime.date(date_obj.year, date_obj.month, 1)
    elif interval == 'year':
        res = datetime.date(date_obj.year, 1, 1)
    else:
        raise Exception("interval must be ['week', 'month', 'year']")
    return res.strftime('%Y-%m-%d')


if __name__ == '__main__':
    # app_name
    file_name = os.path.basename(__file__)
    # 初始化spark环境
    spark = SparkInit(file_name)
    # 注册udf函数，可以在sql中直接使用
    spark.spark.udf.register('udf_date_trunc', date_trunc)

    # 取出目标产品
    sql_target_pro = """          
        select product_id
        from {p_edw_schema}.dim_target_product 
        where day_date = '{p_input_date}'
        group by product_id
    """

    # 取到每个skc在全国范围内，在所有门店中的最早铺货时间
    sql_distributed = """
        select 
            c.product_code
            , c.color_code
            , udf_date_trunc('week', min(a.io_date)) as skc_country_distributed_monday
            , (datediff('{p_input_date}', min(a.io_date))+1) as distributed_days
        from {p_edw_schema}.fct_io a
        inner join (
            select store_id, store_code from {p_edw_schema}.dim_store where is_store = 'Y'
        ) as b 
            on a.org_id = b.store_id
        inner join {p_edw_schema}.dim_product_skc c 
            on a.product_id = c.product_id 
            and a.color_id = c.color_id
        inner join target_pro d 
            on a.product_id = d.product_id
        where 
            a.io_date <= '{p_input_date}' 
            and (a.io_type = '零售' or a.io_type = '调拨入库')
        group by 
            c.product_code, c.color_code
    """

    sql_insert = """
        insert overwrite table {p_dm_schema}.dm_skc_country_day_life_cycle
        partition(day_date)
        select a.product_code
            , a.color_code
            , datediff('{p_input_date}', a.skc_country_distributed_monday)/7+1 as life_cycle_week
            , distributed_days
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from distributed a
    """

    spark.create_temp_table(sql_target_pro, 'target_pro')
    spark.create_temp_table(sql_distributed, 'distributed')
    spark.execute_sql(sql_insert)
    spark.drop_temp_table('target_pro')
    spark.drop_temp_table('distributed')
