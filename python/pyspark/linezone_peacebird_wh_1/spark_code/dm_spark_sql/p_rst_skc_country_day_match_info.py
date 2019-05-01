# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_rst_skc_country_day_match_info
   Description :
   Author :       yangming
   date：          2018/9/12
-------------------------------------------------
   Change Activity:
                   2018/9/12:
-------------------------------------------------
"""
import os

from utils.tools import SparkInit

if __name__ == '__main__':
    # app_name
    file_name = os.path.basename(__file__)

    # 初始化spark环境
    foo = SparkInit(file_name)

    # 创建tmp_dim_target_product,
    sql_dim_target_product_tmp = """
        select product_id, min(tag_price)
        from {p_edw_schema}.dim_target_product
        where day_date='{p_input_date}'
        group by product_id
    """

    # 将一单重复的skc去重，并得到一单出现了多少个不同skc
    sql_tmp1 = """
        select 
            fs.order_id
          , fs.product_id
          , fs.color_id
          , count(1) over (partition by fs.order_id) as counter
        from {p_edw_schema}.fct_sales fs
        inner join {p_edw_schema}.dim_target_store dts
            on fs.store_id = dts.store_id
        where fs.sale_date <= '{p_input_date}'
            and fs.sale_date > date_sub('{p_input_date}',14)
            and fs.qty > 0
        group by fs.order_id,fs.product_id,fs.color_id
    """

    # 得到一单卖出大于1个skc的单子，以及同一个单子的不同skc的组合，搭配款不限制是目标产品，主款要限制是目标产品
    sql_tmp2 = """
        select a.product_id as product_a
            , a.color_id as color_a
            , b.product_id as product_b
            , b.color_id as color_b
            , count(1) as fenzi
        from (select * from tmp1 where counter>1) as a
        inner join dim_target_product_tmp c
            on a.product_id=c.product_id
        inner join (select * from tmp1 where counter>1) as b
            on a.order_id=b.order_id
        -- where concat_ws('+',a.product_id,a.color_id) != concat_ws("+",b.product_id,b.color_id)
        where not(a.product_id = b.product_id and a.color_id = b.color_id)
        group by a.product_id, a.color_id, b.product_id, b.color_id    
    """

    sql = """
        insert overwrite table {p_rst_schema}.rst_skc_country_day_match_info
        partition(day_date)
        select skc.product_code as main_product_code
            , skc.product_name as main_product_name
            , skc.color_code as main_color_code
            , skc.color_name as main_color_name
            , p.tiny_class as main_tiny_class
            , p.tag_price as main_tag_price
            , p.year_id as main_year
            , p.quarter_id as main_quarter
            , skc2.product_code as match_product_code
            , skc2.product_name as match_product_name
            , skc2.color_code as match_color_code
            , skc2.color_name as match_color_name
            , p2.tiny_class as match_tiny_class
            , p2.tag_price as match_tag_price
            , p2.year_id as match_year
            , p2.quarter_id as match_quarter
            , tmp2.fenzi as numerator
            , sum(tmp2.fenzi) over (partition by tmp2.product_a,tmp2.color_a) as denominator
            , current_timestamp as etl_time
            , '{p_input_date}'as day_date
        from tmp2
        inner join {p_edw_schema}.dim_product as p on tmp2.product_a =p.product_id
        inner join {p_edw_schema}.dim_product_skc as skc on tmp2.product_a=skc.product_id
            and tmp2.color_a=skc.color_id
        inner join {p_edw_schema}.dim_product as p2 on tmp2.product_b=p2.product_id
        inner join {p_edw_schema}.dim_product_skc as skc2 on tmp2.product_b=skc2.product_id
            and tmp2.color_b=skc2.color_id
    """

    dim_target_product_tmp = foo.create_temp_table(sql_dim_target_product_tmp, 'dim_target_product_tmp')
    tmp1 = foo.create_temp_table(sql_tmp1, 'tmp1')
    tmp2 = foo.create_temp_table(sql_tmp2, 'tmp2')

    foo.execute_sql(sql)

    foo.drop_temp_table('dim_target_product_tmp')
    foo.drop_temp_table('tmp1')
    foo.drop_temp_table('tmp2')