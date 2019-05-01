# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_rst_store
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
        insert overwrite table {p_rst_schema}.rst_store
        select a.store_id
            , a.store_code
            , a.store_type
            , a.store_kind
            , a.store_name
            , a.store_level
            , a.biz_district
            , a.selling_area
            , a.warehouse_area
            , a.opening_time
            , a.close_time
            , a.status
            , a.province
            , a.city
            , a.city_level
            , a.area
            , a.clerk_count
            , a.address
            , a.city_code
            , a.city_long_code
            , a.dq_code
            , a.dq_long_code
            , b.org_name as dp_name
            , a.nanz_code
            , a.nanz_long_code
            , a.zone_id
            , a.zone_name
            , a.lat
            , a.lng
            , a.is_store
            , a.is_toc as is_toc
            , current_timestamp as etl_time
        from {p_edw_schema}.dim_target_store a
        inner join {p_edw_schema}.dim_stockorg b on a.dq_long_code=b.org_longcode
    """

    foo.execute_sql(sql)
