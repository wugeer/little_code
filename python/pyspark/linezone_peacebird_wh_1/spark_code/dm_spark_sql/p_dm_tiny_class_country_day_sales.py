# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_tiny_class_country_day_sales
# Author: zsm
# Date: 2018/9/11 11:25
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_tiny_class_country_day_sales partition(day_date)
        select b.year_id 
            , b.quarter_id
            , b.tiny_class
            , sum(a.total_sales_qty) as total_sales_qty
            , sum(a.fullprice_total_amt) as fullprice_total_amt
            , sum(a.total_sales_amt) as total_sales_amt
            , sum(a.total_tag_price) as total_tag_price
            , current_timestamp as etl_time 
            , '{p_input_date}'
        from {p_dm_schema}.dm_skc_country_day_sales a 
        inner join {p_edw_schema}.dim_product b 
            on a.product_code = b.product_code
        where 
            day_date = '{p_input_date}'
        group by 
            b.year_id
            , b.quarter_id
            , b.tiny_class
    '''
    spark.execute_sql(sql_insert)