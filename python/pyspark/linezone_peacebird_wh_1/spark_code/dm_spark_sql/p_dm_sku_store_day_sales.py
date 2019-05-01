# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_sku_store_day_sales
   Description :
   Author :       yangming
   date：          2018/9/6
-------------------------------------------------
   Change Activity:
                   2018/9/6:
-------------------------------------------------
"""
import os

from utils.tools import SparkInit

if __name__ == '__main__':
    # app_name
    file_name = os.path.basename(__file__)

    # 初始化spark环境
    foo = SparkInit(file_name)

    # sql部分
    sql = """
            insert overwrite table {p_dm_schema}.dm_sku_store_day_sales
            partition(day_date)
            select dps.product_code
                , dps.color_code
                , dps.size_code
                , ds.store_code
                , sum(case when fs.sale_date = '{p_input_date}' then fs.qty else 0 end) as sales_qty
                , sum(case when fs.sale_date = '{p_input_date}' then fs.real_amt else 0 end) as sales_amt
                , sum(case when fs.sale_date > date_sub('{p_input_date}', 7) then fs.qty else 0 end) 
                        as last_seven_days_sales_qty
                , sum(case when fs.sale_date  > date_sub('{p_input_date}', 7) then fs.real_amt else 0 end) 
                        as last_seven_days_sales_amt
                , sum(case when fs.sale_date > date_sub('{p_input_date}', 14) and 
                        fs.sale_date <= date_sub('{p_input_date}', 7) then fs.qty else 0 end) 
                        as last2_seven_days_sales_qty    -- 上上七天的累计销量
                , sum(case when fs.sale_date > date_sub('{p_input_date}', 30) then fs.qty else 0 end) 
                        as last2_seven_days_sales_qty    -- 过去30天销量
                , sum(fs.qty) as total_sales_qty
                , sum(fs.real_amt) as total_sales_amt
                , current_timestamp as etl_time
                , '{p_input_date}' as day_date
            from {p_edw_schema}.fct_sales as fs 
            inner join {p_edw_schema}.dim_target_store ds       -- 排除加盟门店 的销售数据
                on fs.store_id = ds.store_id
            inner join (select * from {p_edw_schema}.dim_target_product where day_date = '{p_input_date}') b 
                on fs.product_id = b.product_id and ds.dq_long_code = b.dq_long_code
            inner join {p_edw_schema}.dim_product_sku as dps on fs.product_id = dps.product_id 
                    and fs.color_id = dps.color_id and fs.size_id = dps.size_id
            where fs.sale_date <= '{p_input_date}' 
            group by dps.product_code, dps.color_code, ds.store_code, dps.size_code
    """

    foo.execute_sql(sql)