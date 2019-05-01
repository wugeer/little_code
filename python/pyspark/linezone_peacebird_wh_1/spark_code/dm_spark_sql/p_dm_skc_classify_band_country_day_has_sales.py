# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_skc_classify_band_country_day_has_sales
# Author: zsm
# Date: 2018/9/11 17:14
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    # 筛选 非加盟商 所需日期内 销售数据
    sql_tmp_sales_1 = '''
        select
            a.product_id
            , a.color_id
            , a.store_id
            , a.qty
        from {p_edw_schema}.fct_sales a
        inner join  {p_edw_schema}.dim_target_store ts             -- 排除加盟门店的数据
            on a.store_id = ts.store_id 
        where sale_date > date_sub('{p_input_date}', 7) 
            and sale_date <= '{p_input_date}'
    '''

    sql_sales = '''
        select p.prod_class
            , p.stage_lifetime
            , b.year_id
            , b.quarter_id
            , b.band
            , a.store_id
            , sum(a.qty) as last_seven_days_sales_qty
        from tmp_sales_1 a 
        inner join {p_edw_schema}.dim_product b 
            on a.product_id = b.product_id
        inner join (
            select * from {p_edw_schema}.mod_skc_week_classify 
            where cla_week_date = '{p_input_date_mon}'
        ) as p
            on a.product_id = p.product_id 
            and a.color_id = p.color_id
        --where 
        --    sale_date > date_sub('{p_input_date}', 7) 
        --    and sale_date <= '{p_input_date}'
        group by 
            p.prod_class
            , p.stage_lifetime
            , b.year_id
            , b.quarter_id
            , b.band
            , a.store_id
    '''

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_skc_classify_band_country_day_has_sales partition(day_date)
        select prod_class
            , stage_lifetime
            , year_id
            , quarter_id   
            , band
            , sum(case when last_seven_days_sales_qty > 0 then 1 else 0 end) as has_sales_store_qty
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from sales
        group by 
            prod_class
            , stage_lifetime
            , year_id
            , quarter_id, band
    '''

    spark.create_temp_table(sql_tmp_sales_1, 'tmp_sales_1')
    spark.create_temp_table(sql_sales, 'sales')

    spark.execute_sql(sql_insert)

    spark.drop_temp_table('sales')
    spark.drop_temp_table('tmp_sales_1')
