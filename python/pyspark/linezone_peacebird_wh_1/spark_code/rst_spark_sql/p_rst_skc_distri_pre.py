# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_rst_skc_distri_pre
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

    # 创建tmp1, 过去七天销量和累计销量
    sql_tmp1 = """
        select product_code
            , color_code
            , last_seven_days_sales_qty
            , total_sales_qty
        from {p_dm_schema}.dm_skc_country_day_sales
        where day_date = '{p_input_date}'
    """

    sql = """
        insert overwrite table {p_rst_schema}.rst_skc_distri_pre
        partition(day_date)
        select b.product_code
            , b.product_name
            , b.color_code
            , b.color_name
            , a.two_week_sales_celling_qty
            , a.two_week_sales_floor_qty
            , a.four_week_sales_celling_qty
            , a.four_week_sales_floor_qty
            , a.residue_sales_celling_qty
            , a.residue_sales_floor_qty
            , null as two_week_all_distri_sales_celling_qty
            , null as two_week_all_distri_sales_floor_qty
            , null as four_week_all_distri_sales_celling_qty
            , null as four_week_all_distri_sales_floor_qty
            , null as residue_all_distri_sales_celling_qty
            , null as residue_all_distri_sales_floor_qty
            , d.last_seven_days_sales_qty as last_week_sales_qty
            , d.total_sales_qty as total_sales_qty
            , c.org_code
            , c.org_longcode as org_long_code
            , current_timestamp as etl_time
            , a.pre_day_date as day_date
        from {p_edw_schema}.mod_skc_day_sales_prediction a
        inner join {p_edw_schema}.dim_product_skc b on a.product_id = b.product_id
            and a.color_id = b.color_id
        inner join {p_edw_schema}.dim_stockorg c on a.org_id = c.org_id
        inner join tmp1 d on b.product_code = d.product_code and b.color_code = d.color_code
        where a.pre_day_date = '{p_input_date}'
    """

    tmp1 = foo.create_temp_table(sql_tmp1, 'tmp1')

    foo.execute_sql(sql)

    foo.drop_temp_table('tmp1')