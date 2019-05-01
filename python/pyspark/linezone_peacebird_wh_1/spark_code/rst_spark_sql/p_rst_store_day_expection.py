# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_store_day_expection
# Author: zsm
# Date: 2018/9/12 15:02
# ----------------------------------------------------------------------------
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_store_day_expection partition(day_date)
        select
            a.store_code
            , e.store_name
            , a.store_expection
            , a.pla_comple_rate
            , coalesce(a.act_comple_rate,0) as act_comple_rate
            , null as temperate_zone
            , d.org_code as region_code
            , d.org_longcode as region_long_code
            , e.zone_id as zone_code
            , c.org_code as city_code
            , c.org_longcode as city_long_code
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_store_day_mon_section_comple_rate a
        inner join {p_edw_schema}.dim_stockorg b
            on a.store_code = b.org_code
        inner join {p_edw_schema}.dim_stockorg c	
            on b.parent_id = c.org_id
        inner join {p_edw_schema}.dim_stockorg d
            on c.parent_id = d.org_id
        inner join {p_edw_schema}.dim_store e
            on a.store_code = e.store_code
        where
            a.day_date = '{p_input_date}'
    '''
    spark.execute_sql(sql_insert)
