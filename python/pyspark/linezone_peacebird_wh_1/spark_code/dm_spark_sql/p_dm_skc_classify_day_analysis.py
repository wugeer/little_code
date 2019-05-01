# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_skc_classify_day_analysis
# Author: zsm
# Date: 2018/9/11 12:28
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_total_sales = '''
        select b.year_id
            , b.quarter_id
            , c.prod_class
            , c.stage_lifetime
            , '{p_input_date}' as day_date
            , sum(a.qty) as total_sales_qty
        from {p_edw_schema}.fct_sales as a
        inner join {p_edw_schema}.dim_target_store dts    -- 排除掉 销售表中 加盟门店的数据
            on a.store_id = dts.store_id
        inner join {p_edw_schema}.dim_product as b 
            on a.product_id = b.product_id
        inner join (
            select product_id, color_id, prod_class, stage_lifetime
            from {p_edw_schema}.mod_skc_week_classify
            where cla_week_date = '{p_input_date_mon}'
        ) as c
            on a.product_id = c.product_id 
            and a.color_id = c.color_id 
        where 
            a.sale_date <= '{p_input_date}'
        group by 
            b.year_id
            , b.quarter_id
            , c.prod_class
            , c.stage_lifetime
    '''

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_skc_classify_day_analysis partition(day_date)
        select year_id
            , quarter_id
            , prod_class
            , stage_lifetime
            , total_sales_qty
            , total_sales_qty/sum(total_sales_qty) over (partition by year_id, quarter_id, day_date)as sales_rate
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from total_sales 
    '''
    
    spark.create_temp_table(sql_total_sales, 'total_sales')
    spark.execute_sql(sql_insert)
    spark.drop_temp_table('total_sales')
