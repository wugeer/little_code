# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_skc_classify_band_country_on_sales_days
# Author: zsm
# Date: 2018/9/11 17:37
# ----------------------------------------------------------------------------


import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_skc_classify_band_country_on_sales_days partition(day_date)
        select p.prod_class
            , p.stage_lifetime
            , b.year_id
            , b.quarter_id
            , b.band
            , datediff('{p_input_date}', min(io.io_date))+1 as on_sales_days
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from {p_edw_schema}.fct_io io
        inner join {p_edw_schema}.dim_product b 
            on io.product_id=b.product_id
        inner join (
            select * from {p_edw_schema}.mod_skc_week_classify where cla_week_date = '{p_input_date_mon}'
        ) as p
            on io.product_id = p.product_id 
            and io.color_id = p.color_id
        inner join (
            select * from {p_edw_schema}.dim_store where is_store='Y'
        ) ds 
            on ds.store_id = io.org_id   
        where 
            io.io_date <= '{p_input_date}' 
            and (io.io_type = '零售' or io.io_type = '调拨入库')
        group by 
            p.prod_class
            , p.stage_lifetime
            , b.year_id
            , b.quarter_id
            , b.band
    '''
    spark.execute_sql(sql_insert)
