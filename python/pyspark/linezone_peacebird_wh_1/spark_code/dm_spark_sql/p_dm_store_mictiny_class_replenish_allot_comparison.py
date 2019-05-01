# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_{p_dm_schema}_store_mictiny_class_replenish_allot_comparison
# Author: zsm
# Date: 2018/9/11 19:19
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit
from utils.tools import TaskThreadPoolExecutors

if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_sales = '''
        --过去七天销量
        select
            sales.store_code
            , pro.year_id
            , pro.quarter_id
            , pro.mictiny_class
            , min(pro.tiny_class) as tiny_class
            , min(pro.big_class) as big_class
            , sum(sales.last_seven_days_sales_qty) as last_seven_days_sales_qty  
        from {p_dm_schema}.dm_skc_store_day_sales as sales
        inner join {p_edw_schema}.dim_product as pro
            on sales.product_code = pro.product_code
        where 
            sales.day_date = '{p_input_date}'
        group by 
            sales.store_code
            , pro.year_id
            , pro.quarter_id
            , pro.mictiny_class 
    '''

    # --补货量，调入量，调出量
    # --所有skc的补调前库存：available_all_skc_stock_qty
    # --所有skc的补调后库存：after_all_skc_stock_qty
    # --畅销款的补调前库存：available_sell_well_stock_qty
    # --畅销款的补调后库存：after_sell_well_stock_qty
    sql_stock = '''
        select
            stock.store_code
            , pro.year_id
            , pro.quarter_id
            , pro.mictiny_class
            , min(pro.tiny_class) as tiny_class
            , min(pro.big_class) as big_class
            , sum(stock.replenish_qty) as replenish_qty
            , sum(stock.allot_in_qty) as receive_qty
            , sum(stock.allot_out_qty) as send_qty
            , sum(stock.available_stock_qty) as available_all_skc_stock_qty
            , sum(stock.after_replenish_allot_qty) as after_all_skc_stock_qty 
            , sum(case when skc.product_code is not NULL AND available_fullsize.product_code IS NOT null 
                    then stock.available_stock_qty else 0 end
                 ) as available_sell_well_stock_qty
            , sum(case when skc.product_code is not null AND after_fullsize.product_code IS NOT null
                    then stock.after_replenish_allot_qty else 0 end
                 ) as after_sell_well_stock_qty      
        from {p_dm_schema}.dm_sku_store_day_replenish_allot_stock as stock
        inner join {p_edw_schema}.dim_product as pro
            on stock.product_code = pro.product_code
        left join {p_dm_schema}.dm_store_day_sell_well_skc as skc
            on stock.store_code = skc.store_code
            and stock.product_code = skc.product_code
            and stock.color_code = skc.color_code
            and stock.day_date = skc.day_date
        LEFT JOIN {p_dm_schema}.dm_skc_store_day_available_fullsize AS available_fullsize
            ON stock.product_code = available_fullsize.product_code
            AND stock.color_code = available_fullsize.color_code
            AND stock.store_code = available_fullsize.store_code
            AND stock.day_date = available_fullsize.day_date
        LEFT JOIN {p_dm_schema}.dm_skc_store_day_after_replenish_allot_fullsize AS after_fullsize
            ON stock.product_code = after_fullsize.product_code
            AND stock.color_code = after_fullsize.color_code
            AND stock.store_code = after_fullsize.store_code
            AND stock.day_date = after_fullsize.day_date
        where 
            stock.day_date = '{p_input_date}'
        group by 
            stock.store_code
            , pro.year_id
            , pro.quarter_id
            , pro.mictiny_class 
    '''

    # --补调后所有skc个数：after_all_skc_count;
    # --补调后所有skc段码率分母：after_all_skc_count;
    # --补调后所有skc段码率分子：after_all_skc_brokensize_numerator;
    # --补调后畅销款skc个数：after_sell_well_skc_count;
    # --补调后畅销款段码率分母: after_sell_well_skc_count;
    # --补调后畅销款段码率分子：after_sell_well_brokensize_numerator;
    sql_after_ra = '''
        select
            fullsize.store_code
            , pro.year_id
            , pro.quarter_id
            , pro.mictiny_class
            , min(pro.tiny_class) as tiny_class
            , min(pro.big_class) as big_class
            , count(1) as after_all_skc_count 
            , count(case when fullsize.is_fullsize = 'N' 
                        then skc.product_code end
                   ) as after_all_skc_brokensize_numerator
            , count(skc.product_code) as after_sell_well_skc_count 
            , count(case when fullsize.is_fullsize = 'N' and skc.product_code is not null 
                        then skc.product_code end
                   ) as after_sell_well_brokensize_numerator
        from {p_dm_schema}.dm_skc_store_day_after_replenish_allot_fullsize as fullsize
        left join {p_dm_schema}.dm_store_day_sell_well_skc as skc
           on skc.product_code = fullsize.product_code
            and skc.color_code = fullsize.color_code
            and skc.store_code = fullsize.store_code
            and skc.day_date = fullsize.day_date
        inner join {p_edw_schema}.dim_product as pro
            on fullsize.product_code = pro.product_code
        where 
            fullsize.day_date = '{p_input_date}'
        group by 
            fullsize.store_code
            , pro.year_id
            , pro.quarter_id
            , pro.mictiny_class 
    '''

    # --补调前所有skc个数：available_all_skc_count;
    # --补调前所有skc段码率分母：available_all_skc_count;
    # --补调前所有skc段码率分子：available_all_skc_brokensize_numerator;
    # --补调前畅销款skc个数：available_sell_well_skc_count;
    # --补调前畅销款段码率分母: available_sell_well_skc_count;
    # --补调前畅销款段码率分子：available_sell_well_brokensize_numerator;
    sql_before_ra = '''
        select
            fullsize.store_code
            , pro.year_id
            , pro.quarter_id
            , pro.mictiny_class
            , min(pro.tiny_class) as tiny_class
            , min(pro.big_class) as big_class
            , count(1) as available_all_skc_count 
            , count(case when fullsize.available_is_fullsize = 'N'
                        then skc.product_code end
                   ) as available_all_skc_brokensize_numerator
            , count(skc.product_code) as available_sell_well_skc_count 
            , count(case when fullsize.available_is_fullsize = 'N' and skc.product_code is not null
                        then skc.product_code end
                   ) as available_sell_well_brokensize_numerator
        from {p_dm_schema}.dm_skc_store_day_available_fullsize as fullsize 
        left join {p_dm_schema}.dm_store_day_sell_well_skc as skc
            on skc.product_code = fullsize.product_code
            and skc.color_code = fullsize.color_code
            and skc.store_code = fullsize.store_code
            and skc.day_date = fullsize.day_date
        inner join {p_edw_schema}.dim_product as pro
            on fullsize.product_code = pro.product_code
        where 
            fullsize.day_date = '{p_input_date}'
        group by
            fullsize.store_code
            , pro.year_id
            , pro.quarter_id
            , pro.mictiny_class
    '''

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_store_mictiny_class_replenish_allot_comparison partition(day_date)
        select
            stock.store_code
            , stock.year_id as year
            , stock.quarter_id as quarter
            , stock.big_class
            , stock.tiny_class
            , stock.mictiny_class
            , coalesce(sales.last_seven_days_sales_qty,0) as last_seven_days_sales_qty
            , stock.replenish_qty
            , stock.receive_qty
            , stock.send_qty
            , coalesce(before_ra.available_sell_well_skc_count,0) as available_sell_well_skc_count
            , coalesce(before_ra.available_all_skc_count,0) as available_all_skc_count
            , coalesce(stock.available_sell_well_stock_qty,0) as available_sell_well_stock_qty
            , coalesce(stock.available_all_skc_stock_qty,0) as available_all_skc_stock_qty
            , coalesce(before_ra.available_sell_well_brokensize_numerator,0) as available_sell_well_brokensize_numerator
            , before_ra.available_sell_well_skc_count AS available_sell_well_brokensize_denominator --没有分母就是null
            , coalesce(before_ra.available_all_skc_brokensize_numerator,0) AS available_all_brokensize_numerator
            , before_ra.available_all_skc_count AS available_all_brokensize_denominator --没有分母就是null
            , coalesce(after_ra.after_sell_well_skc_count,0) as after_sell_well_skc_count
            , coalesce(after_ra.after_all_skc_count,0) as after_all_skc_count
            , stock.after_sell_well_stock_qty
            , stock.after_all_skc_stock_qty
            , coalesce(after_ra.after_sell_well_brokensize_numerator,0) as after_sell_well_brokensize_numerator
            , after_ra.after_sell_well_skc_count AS after_sell_well_brokensize_denominator --没有分母就是Null
            , coalesce(after_ra.after_all_skc_brokensize_numerator,0) AS after_all_brokensize_numerator 
            , after_ra.after_all_skc_count AS after_all_brokensize_denominator --没有分母就是null
            , current_timestamp AS etl_time
            , '{p_input_date}' AS day_date
        from stock 
        left join sales
            on stock.store_code = sales.store_code
            and stock.year_id = sales.year_id
            and stock.quarter_id = sales.quarter_id
            and stock.mictiny_class = sales.mictiny_class
        left join after_ra
            on stock.store_code = after_ra.store_code
            and stock.year_id = after_ra.year_id
            and stock.quarter_id = after_ra.quarter_id
            and stock.mictiny_class = after_ra.mictiny_class
        left join before_ra
            on stock.store_code = before_ra.store_code
            and stock.year_id = before_ra.year_id
            and stock.quarter_id = before_ra.quarter_id
            and stock.mictiny_class = before_ra.mictiny_class 
    '''

    sql_name_list = [sql_sales, sql_stock, sql_before_ra, sql_after_ra]
    table_name_list = ['sales', 'stock', 'before_ra', 'after_ra']
    multi = TaskThreadPoolExecutors(spark.create_temp_table, 20, sql_name_list, table_name_list)
    # 目的是等待子线程结束再向下执行
    multi.result
    # print(spark.spark.table('sales').show())
    # print(spark.spark.table('stock').show())
    # print(spark.spark.table('before_ra').show())
    # print(spark.spark.table('after_ra').show())
    spark.execute_sql(sql_insert)
    spark.drop_temp_table('sales')
    spark.drop_temp_table('stock')
    spark.drop_temp_table('brfore_ra')
    spark.drop_temp_table('after_ra')
