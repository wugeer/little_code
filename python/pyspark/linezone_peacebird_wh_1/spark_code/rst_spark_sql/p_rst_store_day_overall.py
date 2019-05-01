# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_store_day_overall
# Author: zsm
# Date: 2018/9/12 16:07
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_store_day_overall partition(day_date)
        select
            stock.store_code
            ,store.store_name
            ,store_exp.store_expection
            ,stock.stock_qty
            ,broken.brokensize_rate
            ,sw_broken.sell_well_brokensize_rate
            ,coalesce(sales.last_fourteen_days_sales_qty,0) as two_week_sales_qty
            ,stock.stock_qty / (sales.last_week_sales_qty) as turnover_weeks
            ,(cast(null as string)) as temperate_code
            ,region.org_code as region_code
            ,region.org_longcode as region_long_code
            ,region.org_name as region_name
            ,store.zone_id as zone_code
            ,city.org_code as city_code
            ,city.org_longcode as city_long_code
            ,current_timestamp as etl_time
            ,'{p_input_date}' as day_date
        from {p_dm_schema}.dm_store_day_stock as stock --最终的store包括给了目标营业额的并且在日莫库存中有记录的
        left join {p_dm_schema}.dm_store_day_brokensize_rate as broken --如果这个店所有skc的所有size都库存小于等于0则没有这个门店的记录
            on stock.store_code = broken.store_code
            and stock.day_date = broken.day_date
        left join {p_dm_schema}.dm_store_day_sell_well_brokensize_rate as sw_broken--如果这个店所有skc的所有size都库存小于等于0则没有这个门店
            on stock.store_code = sw_broken.store_code
            and stock.day_date=sw_broken.day_date
        left join {p_dm_schema}.dm_store_day_sales_analysis as sales--如果这个店过去两周都没有销售记录则没有这个门店的记录
            on stock.store_code = sales.store_code
            and stock.day_date = sales.day_date
        inner join {p_edw_schema}.dim_stockorg as store_org
            on stock.store_code = store_org.org_code
        inner join {p_edw_schema}.dim_stockorg as city
            on store_org.parent_id = city.org_id
        inner join {p_edw_schema}.dim_stockorg as region
            on city.parent_id = region.org_id
        inner join {p_edw_schema}.dim_store as store
            on stock.store_code = store.store_code
        inner join {p_dm_schema}.dm_store_day_expection as store_exp--目标营业额表里有这个门店就有这个门店的分类记录
            on stock.store_code = store_exp.store_code
            and stock.day_date = store_exp.day_date
        where
            stock.day_date = '{p_input_date}'
    '''
    spark.execute_sql(sql_insert)
