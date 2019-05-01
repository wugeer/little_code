# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_skc_classify_band_country_day_sales
# Author: zsm
# Date: 2018/9/11 17:20
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_skc_classify_band_country_day_sales partition(day_date)
        select p.prod_class
            , p.stage_lifetime
            , b.year_id
            , b.quarter_id
            , b.band
            , sum(a.total_sales_qty) as total_sales_qty
            , sum(last_seven_days_sales_qty) as last_seven_days_sales_qty
            , current_timestamp as etl_time 
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_skc_country_day_sales as a 
        inner join {p_edw_schema}.dim_product as b 
            on a.product_code = b.product_code
        inner join {p_edw_schema}.dim_product_skc as dps 
            on a.product_code = dps.product_code 
            and a.color_code = dps.color_code
        inner join (
            select * from {p_edw_schema}.mod_skc_week_classify where cla_week_date = '{p_input_date_mon}'
        ) as p
            on dps.product_id = p.product_id 
            and dps.color_id = p.color_id
        where 
            day_date = '{p_input_date}'
        group by 
            p.prod_class
            , p.stage_lifetime
            , b.year_id
            , b.quarter_id
            , b.band
    '''
    spark.execute_sql(sql_insert)
