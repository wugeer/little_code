# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_org_day_mon_section_target_sales_amt
# Author: zsm
# Date: 2018/9/5 16:05
# ----------------------------------------------------------------------------

# -*- coding: utf-8 -*-
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)
    sql = '''
    insert overwrite table {p_dm_schema}.dm_org_day_mon_section_target_sales_amt partition(day_date)
    select
        a.org_code
        , a.org_long_code 
        , a.org_type
        , sum(a.target_amt) over(partition by a.org_id order by a.day_date) as target_mon_section_sales_amt--本月一号到今天的目标销售额
        , current_timestamp as etl_time
        , a.day_date
    from {p_edw_schema}.mid_org_day_target_amt a
    where
        a.day_date <= last_day('{p_input_date}')--本月末
--        a.day_date <= '{p_input_date}'
        and a.day_date >= trunc('{p_input_date}','MM')--本月1号  
    '''
    spark.execute_sql(sql)
