# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_skc_country_order
# Author: zsm
# Date: 2018/9/11 11:07
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_target_pro = '''
        select min(year_id) as year_id
            , min(quarter_id) as quarter_id
            , min(tiny_class) as tiny_class
        ,product_id
        from {p_edw_schema}.dim_target_product 
        where day_date = '{p_input_date}'
        group by product_id
    '''

    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_skc_country_order 
        select b.product_code
            , b.color_code
            , a.otb_order_qty as total_otb_order_qty
            , rank() over(partition by c.year_id,c.quarter_id order by a.otb_order_qty desc) as total_otb_order_rank
            , rank() over(partition by c.year_id,c.quarter_id,c.tiny_class order by a.otb_order_qty desc) as total_otb_order_in_tinyclass_rank
            , current_timestamp as etl_time 
        from {p_edw_schema}.dim_skc_otb_order as a 
        inner join {p_edw_schema}.dim_product_skc as b 
            on a.product_id = b.product_id 
            and a.color_id = b.color_id
        inner join target_pro as c 
            on a.product_id = c.product_id
    '''

    spark.create_temp_table(sql_target_pro, 'target_pro')
    spark.execute_sql(sql_insert)
    spark.drop_temp_table('target_pro')
