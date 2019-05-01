# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_skc_country_day_sales
   Description :
   Author :       yangming
   date：          2018/9/10
-------------------------------------------------
   Change Activity:
                   2018/9/10:
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
    sql_tmp1 = """          
        select min(tag_price) as tag_price,product_id
        from {p_edw_schema}.dim_target_product 
        where day_date = '{p_input_date}'
        group by product_id
    """

    foo.create_temp_table(sql_tmp1, 'tmp1')

    sql = """
            insert overwrite table {p_dm_schema}.dm_skc_country_day_sales
            partition(day_date)
            select dps.product_code
                , dps.color_code
                , min(fs.sale_date) as first_sale_date  -- 当季skc首销日期
                , sum(case when fs.sale_date = '{p_input_date}' then fs.qty else 0 end) as sales_qty   -- 当天销量
                , sum(case when fs.sale_date = '{p_input_date}' then fs.real_amt else 0 end) as sales_amt  -- 当天销售额
                , sum(fs.qty) as total_sales_qty  -- 当季累计销量
                , sum(fs.real_amt) as total_sales_amt -- 当季累计销售额
                , sum(case when (fs.real_price/b.tag_price) >= {p_full_price_rate} then fs.real_amt else 0 end) 
                    as fullprice_total_amt   -- 正价累计销售额
                , sum(fs.qty*b.tag_price) as total_tag_price        --累计吊牌价之和
                , sum(case when fs.sale_date > date_sub('{p_input_date}', 7) then fs.qty else 0 end) 
                    as last_seven_days_sales_qty   --取过去7天的累计销量
                , sum(case when fs.sale_date >= '{p_input_date_mon}' then fs.qty else 0 end) 
                    as week_sales_qty   --本周(截止到当前)销量
                , sum(case when fs.sale_date < '{p_input_date_mon}' and 
                        fs.sale_date >= date_sub('{p_input_date_mon}', 7) then fs.qty else 0 end) 
                    as last_week_sales_qty   --上周销量  
                , sum(case when fs.sale_date < date_sub('{p_input_date_mon}', 7) and 
                        fs.sale_date >= date_sub('{p_input_date_mon}', 14) then fs.qty else 0 end) 
                    as last_two_week_sales_qty       -- '上两周销量' 
                , sum(case when fs.sale_date<date_sub('{p_input_date_mon}', 14) and 
                        fs.sale_date >= date_sub('{p_input_date_mon}', 21) then fs.qty else 0 end) 
                    as last_three_week_sales_qty     -- '上三周销量' 
                , sum(case when fs.sale_date<date_sub('{p_input_date_mon}', 21) and 
                        fs.sale_date>=date_sub('{p_input_date_mon}', 28) then fs.qty else 0 end) 
                    as last_four_week_sales_qty      -- '上四周销量' 
                , sum(case when fs.sale_date<date_sub('{p_input_date_mon}', 28) and 
                        fs.sale_date>=date_sub('{p_input_date_mon}', 35) then fs.qty else 0 end) 
                    as last_five_week_sales_qty       -- '上五周销量'   
                , current_timestamp as etl_time
                , '{p_input_date}' as day_date
            from {p_edw_schema}.fct_sales as fs
            inner join {p_edw_schema}.dim_target_store dts       -- 排除掉 加盟门店 的销售数据
                on fs.store_id = dts.store_id
            inner join tmp1 b on fs.product_id = b.product_id
            inner join {p_edw_schema}.dim_product_skc as dps  -- 取颜色编码
                on fs.product_id = dps.product_id and fs.color_id = dps.color_id
            where fs.sale_date <= '{p_input_date}'
            group by dps.product_code, dps.color_code    
    """

    foo.execute_sql(sql)

    foo.drop_temp_table('tmp1')