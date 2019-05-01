# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: dm_skc_city_day_sales_toc
# Author: zsm
# Date: 2018/9/10 12:50
# ----------------------------------------------------------------------------


import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql = '''
        insert overwrite table {p_dm_schema}.dm_skc_city_day_sales_toc partition(day_date)
        select a.product_code
            , a.color_code
            , max(b.city_code) as city_code
            , b.city_long_code as city_long_code
            , b.is_toc
            , sum(a.sales_qty) as sales_qty
            , sum(a.sales_amt) as sales_amt
            , sum(a.last_seven_days_sales_qty) as last_seven_days_sales_qty
            , sum(a.last_seven_days_sales_amt) as last_seven_days_sales_amt
            , sum(a.last_fourteen_days_sales_qty) as last_fourteen_days_sales_qty
            , sum(a.last_fourteen_days_sales_amt) as last_fourteen_days_sales_amt
            , sum(a.his_sales_qty) as his_sales_qty
            , sum(a.his_sales_amt) as his_sales_amt
            , sum(a.last_week_sales_qty) as last_week_sales_qty
            , sum(a.last_week_sales_amt) as last_week_sales_amt
            , sum(a.week_sales_qty) as week_sales_qty
            , sum(a.week_sales_amt) as week_sales_amt
            , sum(a.total_sales_qty) as total_sales_qty
            , sum(a.total_sales_amt) as total_sales_amt
            , sum(a.total_sales_qty * d.tag_price) as total_tag_price
            , current_timestamp as etl_time 
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_skc_store_day_sales a 
        inner join {p_edw_schema}.dim_store b 
            on a.store_code = b.store_code
        inner join {p_edw_schema}.dim_product d 
            on a.product_code = d.product_code
        where 
            day_date = '{p_input_date}'
        group by 
            a.product_code
            , a.color_code
            , b.city_long_code
            , b.is_toc
    '''

    spark.execute_sql(sql)
