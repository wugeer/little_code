# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_comp_store_sales_analysis
# Author: zsm
# Date: 2018/9/11 17:56
# ----------------------------------------------------------------------------

import os
from datetime import timedelta
import datetime
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
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)
    # 注册udf函数，可以在sql中直接使用
    spark.spark.udf.register('udf_date_trunc', date_trunc)

    sql_tmp_dim_ud_date = '''
        select 
            a.day_date
            , (case
                    when b.day_date is null and a.month_id=1 then concat(a.year_id-1,'-01-01')
                    when b.day_date is null and a.month_id=12 then concat(a.year_id-1,'-12-31')
                    else b.day_date
            end) as last_day_date 
        from {p_edw_schema}.dim_ud_date a
        left join {p_edw_schema}.dim_ud_date b
            on a.year_id = b.year_id + 1 
            and a.week_id = b.week_id
            and a.weekday_id = b.weekday_id
         where 
             a.day_date >= '2018-01-01'
    '''

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_comp_store_sales_analysis partition(day_date)
        select            
            -- 上一年年初到上一年对应日期的累计销售业绩
            sum(case when fs.sale_date <= tdud.last_day_date then fs.real_amt else 0 end) as last_total_sales_amt
            -- 当天的年初到当天的累计销售业绩
            , sum(case when fs.sale_date >= '{p_input_date_year}' then fs.real_amt else 0 end) as total_sales_amt
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from {p_edw_schema}.fct_sales fs 
        --取可比门店的销售业绩 
        inner join {p_edw_schema}.dim_comp_store dcs
            on fs.store_id = dcs.comp_store_id
            and dcs.day_date = '{p_input_date}'
        --取与当天时间按第几周的星期几所对应的上一年的日期
        left join tmp_dim_ud_date tdud
            on tdud.day_date = '{p_input_date}' 
        --只销售日期在上一年第一天到当天这个时间段的销售情况
        where 
            fs.sale_date <= '{p_input_date}' 
            and fs.sale_date >= udf_date_trunc('year', tdud.last_day_date)

    '''

    spark.create_temp_table(sql_tmp_dim_ud_date, 'tmp_dim_ud_date')
    spark.execute_sql(sql_insert)
    spark.drop_temp_table('tmp_dim_ud_date')
