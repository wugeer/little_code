# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_sku_store_day_replenish_allot_stock
   Description :
   Author :       yangming
   date：          2018/9/6
-------------------------------------------------
   Change Activity:
                   2018/9/6:
-------------------------------------------------
"""

import os

from utils.tools import SparkInit

if __name__ == '__main__':
    # app_name
    file_name = os.path.basename(__file__)

    # 初始化spark环境
    foo = SparkInit(file_name)

    sql_stock = """
        select 
            a.product_code
            , a.color_code
            , a.size_code --sku
            , a.store_code --门店
            , a.available_stock_qty --加在途后库存数量
            , 0 as replenish_qty --补货量
            , 0 as allot_out_qty --调出量
            , 0 as allot_in_qty --调入量
            , 0 as return_qty --返仓量
            , 0 as road_stock_qty --在途库存
        from {p_dm_schema}.dm_sku_store_day_available_stock a
        where a.day_date = '{p_input_date}'
        union all 
        select 
              sku.product_code
            , sku.color_code
            , sku.size_code --sku
            , store.store_code --门店
            , 0 as available_stock_qty --加在途后库存数量
            , rep.send_qty as replenish_qty --补货量
            , 0 as allot_out_qty --调出量
            , 0 as allot_in_qty --调入量
            , 0 as return_qty --返仓量
            , 0 as road_stock_qty --在途库存
        from {p_edw_schema}.mod_sku_day_replenish as rep
        inner join {p_edw_schema}.dim_target_store as store
            on rep.receive_store_id = store.store_id
        inner join {p_edw_schema}.dim_product_sku as sku
            on rep.product_id = sku.product_id
            and rep.color_id = sku.color_id
            and rep.size_id = sku.size_id
        where rep.dec_day_date = date_add('{p_input_date}',1)
        union all 
        select 
            sku.product_code
            , sku.color_code
            , sku.size_code --sku
            , store.store_code --门店
            , 0 as available_stock_qty --加在途后库存数量
            , 0 as replenish_qty --补货量
            , 0 as allot_out_qty --调出量
            , allot.send_qty as allot_in_qty --调入量
            , 0 as return_qty --返仓量
            , 0 as road_stock_qty --在途库存
        from {p_edw_schema}.mod_sku_day_allot as allot
        inner join {p_edw_schema}.dim_target_store as store
            on allot.receive_store_id = store.store_id
        inner join {p_edw_schema}.dim_product_sku as sku
            on allot.product_id = sku.product_id
            and allot.color_id = sku.color_id
            and allot.size_id = sku.size_id
        where allot.dec_day_date = date_add('{p_input_date}',1)
        union all 
        select
            sku.product_code
            , sku.color_code
            , sku.size_code --sku
            , store.store_code --门店
            , 0 as available_stock_qty --加在途后库存数量
            , 0 as replenish_qty --补货量
            , allot.send_qty as allot_out_qty --调出量
            , 0 as allot_in_qty --调入量
            , 0 as return_qty --返仓量
            , 0 as road_stock_qty --在途库存
        from {p_edw_schema}.mod_sku_day_allot as allot
        inner join {p_edw_schema}.dim_target_store as store
            on allot.send_store_id = store.store_id
        inner join {p_edw_schema}.dim_product_sku as sku
            on allot.product_id = sku.product_id
            and allot.color_id = sku.color_id
            and allot.size_id = sku.size_id
        where allot.dec_day_date = date_add('{p_input_date}',1)
        union all 
       select 
              sku.product_code
            , sku.color_code
            , sku.size_code --sku
            , store.store_code --门店
            , 0 as available_stock_qty --加在途后库存数量
            , 0 as retlenish_qty --补货量
            , 0 as allot_out_qty --调出量
            , 0 as allot_in_qty --调入量
            , ret.send_qty as return_qty --返仓量
            , 0 as road_stock_qty --在途库存
        from {p_edw_schema}.mod_sku_day_return_result as ret
        inner join {p_edw_schema}.dim_target_store as store
            on ret.send_store_id = store.store_id
        inner join {p_edw_schema}.dim_product_sku as sku
            on ret.product_id = sku.product_id
            and ret.color_id = sku.color_id
            and ret.size_id = sku.size_id
        where ret.dec_day_date = date_add('{p_input_date}',1)
        union all 
        select
            a.product_code
            , a.color_code
            , a.size_code --sku
            , a.store_code --门店
            , 0 as available_stock_qty --加在途后库存数量
            , 0 as replenish_qty --补货量
            , 0 as allot_out_qty --调出量
            , 0 as allot_in_qty --调入量
            , 0 as return_qty --返仓量
            , a.road_stock_qty as road_stock_qty --在途库存
        from {p_dm_schema}.dm_sku_store_day_road_stock a
        where a.day_date = '{p_input_date}'
    """

    stock = foo.create_temp_table(sql_stock, 'stock')

    sql = """
        insert overwrite table {p_dm_schema}.dm_sku_store_day_replenish_allot_stock partition(day_date) --{p_dm_schema}
        select
            product_code
            , color_code 
            , size_code
            , store_code
            , sum(replenish_qty) as replenish_qty
            , sum(allot_out_qty) as allot_out_qty
            , sum(allot_in_qty) as allot_in_qty
            , sum(available_stock_qty) as available_stock_qty
            , sum(road_stock_qty) as road_stock_qty
            , (sum(replenish_qty) + sum(allot_in_qty) - sum(allot_out_qty) + sum(available_stock_qty) - sum(return_qty))
                as after_replenish_allot_qty                                                                --补调后库存
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from stock
        group by product_code,color_code,size_code,store_code     
    """

    foo.execute_sql(sql)

    foo.drop_temp_table('stock')

