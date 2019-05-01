# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_mid_store_day_sell_well_skc
# Author: lh
# Date: 2018/9/25 14:37
# ----------------------------------------------------------------------------


import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_skc_store_day_sales = """
        select dps.product_code
            , dps.color_code
            , ds.store_code
            , sum(case when fs.sale_date  > date_sub('{p_input_date}', 14) then fs.real_amt else 0 end) 
                    as last_fourteen_days_sales_amt
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from {p_edw_schema}.fct_sales as fs
        inner join {p_edw_schema}.dim_store ds on fs.store_id = ds.store_id
        inner join (select * from {p_edw_schema}.dim_target_product where day_date = '{p_input_date}') b 
            on fs.product_id = b.product_id and ds.dq_long_code = b.dq_long_code
        inner join {p_edw_schema}.dim_product_skc as dps 
            on fs.product_id = dps.product_id and fs.color_id = dps.color_id
        where fs.sale_date <= '{p_input_date}'
        group by dps.product_code, dps.color_code, ds.store_code
    """
    
    sql_sell_well_skc = '''
        
        select store_code
            , product_code
            , color_code
            , sum(last_fourteen_days_sales_amt) over (partition by store_code order by last_fourteen_days_sales_amt desc) as part_amt
            , sum(last_fourteen_days_sales_amt) over (partition by store_code) as total_amt
            , rank() over (partition by store_code order by last_fourteen_days_sales_amt desc) as order_num
        from skc_store_day_sales 
        where 
            day_date = '{p_input_date}'
    '''

    sql_insert = '''
        insert overwrite table {p_edw_schema}.mid_store_day_sell_well_skc PARTITION(day_date)
        select store_code
            , product_code
            , color_code
            , order_num
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from sell_well_skc
        where
            part_amt - total_amt * {p_sell_well_rate} <= 0
    '''
    
    spark.create_temp_table(sql_skc_store_day_sales, 'skc_store_day_sales')
    spark.create_temp_table(sql_sell_well_skc, 'sell_well_skc')
    spark.execute_sql(sql_insert)
    spark.drop_temp_table('sell_well_skc')
    spark.drop_temp_table('skc_store_day_sales')