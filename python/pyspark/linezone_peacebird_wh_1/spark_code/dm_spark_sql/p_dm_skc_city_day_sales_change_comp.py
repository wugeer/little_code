# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_skc_city_day_sales_change_comp
# Author: lh
# Date: 2018/10/10 19:30
# ---------------------------------------------------------------------------- 


#  date_add('{p_input_date}',1)   哪天运行 存哪天的日期  与销量增长损失表一致


import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)   
    
    sql_insert = '''
        insert overwrite table {p_dm_schema}.dm_skc_city_day_sales_change_comp partition(day_date)
        SELECT 
            c.product_code
            , c.color_code
            , b.org_code as city_code
            , b.org_longcode as city_long_code
            , (case 
                    when mod_sales_growth_loss >= 0 then cast(mod_sales_growth_loss as int)
                    else 0
               end
                ) as ai_sales_grow
            , (case 
                    when act_sales_growth_loss >= 0 then cast(act_sales_growth_loss as int)
                    else 0
               end 
                ) as peacebird_sales_grow
            , (case 
                    when mod_sales_growth_loss < 0 then -1*cast(mod_sales_growth_loss as int)
                    else 0
               end
                ) as ai_sales_loss
            , (case 
                    when act_sales_growth_loss < 0 then -1*cast(act_sales_growth_loss as int)
                    else 0
               end
                ) as peacebird_sales_loss
            , current_timestamp as etl_time
            , a.day_date        -- 取 传入日期 + 1 天作为日期，与 销量增长损失表日期保持一致
        from {p_edw_schema}.mod_skc_city_day_sales_grow_loss a 
        inner join {p_edw_schema}.dim_stockorg b 
            on a.city_id = b.org_id
        inner join {p_edw_schema}.dim_product_skc c 
            on a.product_id = c.product_id
                and a.color_id = c.color_id
        where day_date = date_add('{p_input_date}',1)                     
    '''
    
    spark.execute_sql(sql_insert) 