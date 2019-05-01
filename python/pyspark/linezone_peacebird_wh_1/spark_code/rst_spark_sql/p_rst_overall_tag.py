# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_overall_tag
# Author: zsm
# Date: 2018/9/12 16:58
# ----------------------------------------------------------------------------
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_tmp1 = '''
        select
            a.year_id as year
            , a.quarter_id as quarter
            , a.day_date as day_date
            --, (a.brokensize_store_count*1.0/a.store_count) as brokensize_rate  -- 断码率
            , ( case     -- 考虑分子等于0的情况
                    when store_count <> 0 then a.brokensize_store_count*1.0/a.store_count
                    else 0					
                 end
              ) as brokensize_rate  -- 断码率
            --, (a.total_sales_qty*1.0/(a.total_sales_qty+a.road_stock_qty+a.warehouse_total_stock_qty+a.store_total_stock_qty)) as sales_out_rate  -- 售罄率 = 累计销量/(累计销量+在途库存+总仓库存+门店总库存)
            , ( case     -- 考虑分子等于0的情况
                    when (a.total_sales_qty+a.road_stock_qty
                            +a.warehouse_total_stock_qty+a.store_total_stock_qty) <> 0 
                            then a.total_sales_qty*1.0/(a.total_sales_qty+a.road_stock_qty
                            +a.warehouse_total_stock_qty+a.store_total_stock_qty)
                    else 0					
                end
              ) as sales_out_rate
            ,current_timestamp as etl_time 
        from {p_dm_schema}.dm_country_day_analysis a
        where a.day_date = '{p_input_date}'
    '''

    sql_tmp2 = '''
        select 
            day_date
            , (total_sales_amt - last_total_sales_amt)/last_total_sales_amt as comp_store_perf_growth
        from {p_dm_schema}.dm_comp_store_sales_analysis
        where day_date = '{p_input_date}'
    '''

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_overall_tag partition(day_date)
        select
            a.year
            , a.quarter
            , a.brokensize_rate  -- 断码率
            , a.sales_out_rate
            , b.comp_store_perf_growth  
            , current_timestamp as etl_time 
            , '{p_input_date}' as day_date
        from tmp1 a
        left join tmp2 b
            on a.day_date = b.day_date
    '''

    spark.create_temp_table(sql_tmp1, 'tmp1')
    spark.create_temp_table(sql_tmp2, 'tmp2')
    spark.execute_sql(sql_insert)
