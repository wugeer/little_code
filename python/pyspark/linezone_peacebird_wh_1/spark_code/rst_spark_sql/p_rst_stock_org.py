# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_rst_stock_org
   Description :
   Author :       yangming
   date：          2018/9/15
-------------------------------------------------
   Change Activity:
                   2018/9/15:
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
        insert overwrite table {p_rst_schema}.rst_stock_org
        select org_name
             , org_code
             , org_id
             , org_longname
             , org_longcode
             , parent_code
             , parent_id
             , org_type
             , org_typecode
             , status
             , remark
             , current_timestamp as etl_time
        from {p_edw_schema}.dim_stockorg
    """

    foo.execute_sql(sql)