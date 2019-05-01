# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_rst_product
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

    sql = """
        insert overwrite table {p_rst_schema}.rst_product
        select a.product_id
            , a.product_code
            , a.product_name
            , b.color_id
            , b.color_code
            , b.color_name
            , b.size_id
            , b.size_code
            , b.size_name
            , a.year_id as year 
            , a.quarter_id as quarter
            , a.big_class
            , a.mid_class
            , a.tiny_class
            , a.mictiny_class
            , a.brand
            , a.band
            , a.cost_price
            , a.tag_price
            , a.gender
            , a.put_on_date
            , a.pull_off_date
            , current_timestamp as etl_time
        from {p_edw_schema}.dim_product a
        inner join {p_edw_schema}.dim_product_sku b on a.product_id = b.product_id
    """

    foo.execute_sql(sql)