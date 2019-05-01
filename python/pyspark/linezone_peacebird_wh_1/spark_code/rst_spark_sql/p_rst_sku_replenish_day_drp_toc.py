# -*- coding: utf-8 -*-

# --------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_sku_replenish_day_drp
# Author: liuhai
# Date: 2018/11/13 16:38
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit

if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)
    
    sql_target_peacebird = '''
        SELECT 
            org_id, product_id, color_id, size_id , stock_qty, active
        FROM edw.mid_day_target_stock_peacebird
        where stock_date = '{p_input_date}' and stock_qty > 0 and active = 'Y'
        group by org_id, product_id, color_id, size_id , stock_qty, active
        --having sum(stock_qty) > 0    -- 取单个门店，单个skc 的库存数量大于0 的单款 单 skc   
    '''
    
    # 将 send_org_code+receive_org_code+billdate 拼成所需的 refno 
    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_sku_replenish_day_drp_toc partition(billdate)
        select 
            ds.store_code as send_org_code
            , ds1.store_code as receive_org_code
            , concat(ds.store_code,'_',ds1.store_code,'_',from_unixtime(unix_timestamp('{p_input_date_add_one_day}', 'yyyy-mm-dd'), 'yyyymmdd')) as refno
            , dpc.sku_code
            , ssd.qty_actual as qty
            , (
                case when sjv.limit_qty is not null then sjv.limit_qty
                else 3
                end
                ) as receive_org_limt      -- 如果为空 定为 3 
            , sum(ssd.qty_actual) over(partition by ds.store_code, ds1.store_code)  as package_qty_sum             
            , current_timestamp as etl_time
            , from_unixtime(unix_timestamp('{p_input_date_add_one_day}', 'yyyy-mm-dd'), 'yyyymmdd') as billdate
        from peacebird.st_sku_deliver ssd 
        inner join {p_edw_schema}.dim_store ds 
            on ssd.c_orig_id = ds.store_id
        inner join {p_edw_schema}.dim_store ds1 
            on ssd.c_dest_id = ds1.store_id
        inner join {p_edw_schema}.dim_product_sku dpc
            on ssd.skuid = dpc.sku_id
                and ssd.m_product_id = dpc.product_id 
        inner join target_peacebird tp 
            on ssd.c_dest_id = tp.org_id 
                and dpc.product_id = tp.product_id 
                and dpc.color_id = tp.color_id
                and dpc.size_id = tp.size_id
        left join peacebird.st_joinstore_view sjv
            on ssd.c_dest_id = sjv.c_store_id
    '''
    
    spark.create_temp_table(sql_target_peacebird, 'target_peacebird')
    
    spark.execute_sql(sql_insert)
    
    spark.drop_temp_table('target_peacebird')
    
