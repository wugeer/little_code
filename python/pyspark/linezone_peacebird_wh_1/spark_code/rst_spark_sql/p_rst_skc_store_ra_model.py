# -*- coding: utf-8 -*-

# --------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_skc_store_ra_model
# Author: zsm
# Date: 2018/9/12 14:01
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit

if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_end_stock_1 = '''
        select a.product_code,a.color_code,a.size_code            
            ,a.store_code                                    
            ,b.size_name
            ,a.stock_qty     
            ,'{p_input_date}' as day_date
        from {p_dm_schema}.dm_sku_store_day_stock a
        inner join {p_edw_schema}.dim_product_sku b 
            on a.product_code = b.product_code 
            and a.color_code = b.color_code
            and a.size_code = b.size_code
        where day_date = '{p_input_date}' 
    '''

    sql_end_stock_2 = '''
        select product_code,color_code,store_code,size_code,day_date
            ,concat(size_name,'+',stock_qty,'+',size_code) as stock_qty
        from end_stock_1
    '''

    sql_end_stock_3 = '''
        select product_code,color_code,store_code,day_date
            ,concat_ws(',',collect_set(stock_qty)) as stock_qty
        from end_stock_2
        group by product_code,color_code,store_code,day_date
    '''

    sql_road_stock_1 = '''
        select a.product_code,a.color_code,a.size_code                --sku
            ,a.store_code                                            --门店
            ,b.size_name
            ,a.road_stock_qty    --在途库存
            ,'{p_input_date}'  as day_date
        from {p_dm_schema}.dm_sku_store_day_road_stock a 
        inner join {p_edw_schema}.dim_product_sku b 
            on a.product_code = b.product_code 
            and a.color_code = b.color_code
            and a.size_code = b.size_code
        where day_date = '{p_input_date}' 
    '''

    sql_road_stock_2 = '''
        select product_code,color_code,store_code,size_code,day_date
            ,concat(size_name,'+',road_stock_qty,'+',size_code) as road_stock_qty
        from road_stock_1
    '''

    sql_road_stock_3 = '''
        select product_code,color_code,store_code,day_date
            ,concat_ws(',',collect_set(road_stock_qty)) as road_stock_qty
        from road_stock_2
        group by product_code,color_code,store_code,day_date
    '''

    sql_sales_1 = '''
        select a.product_code,a.color_code,a.size_code                --sku
            ,a.store_code                                            --门店
            ,b.size_name
            ,'{p_input_date}' as day_date
            ,a.last_seven_days_sales_qty as week_sales_qty        --近一周销量
            ,total_sales_qty
        from {p_dm_schema}.dm_sku_store_day_sales a 
        inner join {p_edw_schema}.dim_product_sku b 
            on a.product_code = b.product_code 
            and a.color_code = b.color_code
            and a.size_code = b.size_code
        where day_date = '{p_input_date}'
    '''

    sql_sales_2 = '''
        select product_code,color_code,store_code,size_code,day_date,total_sales_qty
            ,concat(size_name,'+',week_sales_qty,'+',size_code) as week_sales_qty
            ,concat(size_name,'+',total_sales_qty,'+',size_code) as total_sales_qty_str
            ,week_sales_qty as week_sales_qty_int
        from sales_1
    '''

    sql_sales_3 = '''
        select product_code,color_code,store_code,day_date
            ,sum(total_sales_qty) as total_sales_qty
            ,concat_ws(',',collect_set(week_sales_qty)) as week_sales_qty
            ,concat_ws(',',collect_set(total_sales_qty_str)) as total_sales_qty_str
            ,sum(week_sales_qty_int) as week_sales_qty_int    
        from sales_2
        group by product_code,color_code,store_code,day_date
    '''

    sql_target_stock_1 = '''
        select b.product_code,b.color_code,b.size_code                        --sku
            ,ds.store_code                                                    --门店
            ,b.size_name
            ,a.target_stock as target_inventory_qty    --目标库存
            ,'{p_input_date}' as day_date
        from {p_edw_schema}.mod_sku_store_day_target_inventory a 
        inner join {p_edw_schema}.dim_target_store ds 
            on a.store_id = ds.store_id
        inner join {p_edw_schema}.dim_product_sku b 
            on a.product_id = b.product_id 
            and a.color_id = b.color_id
            and a.size_id = b.size_id
        inner join {p_edw_schema}.dim_product p 
            on a.product_id=p.product_id
        inner join (
            select * from {p_edw_schema}.dim_target_quarter where day_date='{p_input_date}'
        ) as target
            on p.year_id=target.year_id
            and p.quarter_id=target.quarter_id
        where a.day_date = date_add('{p_input_date}',1)
    '''

    sql_target_stock_2 = '''
        select product_code,color_code,store_code,size_code,day_date    --sku、门店
            ,concat(size_name,'+',target_inventory_qty,'+',size_code) as target_inventory_qty    --目标库存
        from target_stock_1
    '''

    sql_target_stock_3 = '''
        select product_code,color_code,store_code,day_date                --sku、门店
            ,concat_ws(',',collect_set(target_inventory_qty)) as target_inventory_qty    --目标库存
        from target_stock_2
        group by product_code,color_code,store_code,day_date
    '''

    sql_replenish_1 = '''
        select c.product_code,c.color_code,c.size_code                --sku
            ,c.size_name                           --尺码名称
            ,h.store_code as send_org_code                            --发货组织
            ,ds.store_code as receive_org_code                            --收货组织
            --,a.send_qty as suggest_replenish_qty    --发货数量（补货量）
            ,cast(a.send_qty as int) as suggest_replenish_qty    --发货数量（补货量）
            ,'{p_input_date}' as day_date
        from {p_edw_schema}.mod_sku_day_replenish a
        inner join {p_edw_schema}.dim_target_store ds 
            on a.receive_store_id = ds.store_id
        inner join {p_edw_schema}.dim_target_store h 
            on a.send_org_id = h.store_id
        inner join {p_edw_schema}.dim_product_sku c 
            on a.product_id = c.product_id 
            and a.color_id = c.color_id
            and a.size_id = c.size_id
        where a.dec_day_date = date_add('{p_input_date}',1) 
    '''

    sql_replenish_2 = '''
        select product_code,color_code,size_code,send_org_code,receive_org_code
            ,concat(size_name,'+',suggest_replenish_qty,'+',size_code) as suggest_replenish_qty
            ,suggest_replenish_qty as suggest_replenish_qty_int
            ,day_date
        from replenish_1
    '''

    sql_replenish_3 = '''
        select product_code,color_code,send_org_code,receive_org_code,day_date
            ,concat_ws(',',collect_set(suggest_replenish_qty)) as suggest_replenish_qty
            ,sum(suggest_replenish_qty_int) as suggest_replenish_qty_int
        from replenish_2
        group by product_code,color_code,send_org_code,receive_org_code,day_date
    '''

    sql_allot_1 = '''
        select c.product_code,c.color_code,c.size_code            --sku
            ,c.size_name
            ,ds.store_code as send_org_code            --发货组织
            ,ds1.store_code as receive_org_code            --收货组织
            ,a.send_qty as suggest_allot_qty                                --发货数量（调出量）
            ,'{p_input_date}' as day_date
        from {p_edw_schema}.mod_sku_day_allot a 
        inner join {p_edw_schema}.dim_target_store ds 
            on a.send_store_id = ds.store_id
        inner join {p_edw_schema}.dim_target_store ds1 
            on a.receive_store_id = ds1.store_id
        inner join {p_edw_schema}.dim_product_sku c 
            on a.product_id = c.product_id 
            and a.color_id = c.color_id
            and a.size_id = c.size_id
        where a.dec_day_date = date_add('{p_input_date}',1)
    '''

    sql_allot_2 = '''
        select product_code,color_code,size_code,send_org_code,receive_org_code
            ,concat(size_name,'+',suggest_allot_qty,'+',size_code) as suggest_allot_qty
            ,suggest_allot_qty as suggest_allot_qty_int
            ,day_date
        from allot_1
    '''

    sql_allot_3 = '''
        select product_code,color_code               --skc
            ,send_org_code,receive_org_code,day_date            --调出组织、调入组织
            ,concat_ws(',',collect_set(suggest_allot_qty)) as suggest_allot_qty
            ,sum(suggest_allot_qty_int) as suggest_allot_qty_int
        from allot_2
        group by product_code,color_code,send_org_code,receive_org_code,day_date
    '''

    sql_return_1 = '''
        select c.product_code,c.color_code,c.size_code                --sku
            ,c.size_name                           --尺码名称
            ,s.store_code as send_org_code                            --发货组织
            ,h.store_code as receive_org_code                            --收货组织
            ,a.send_qty as suggest_return_qty    --返仓数量
            ,'{p_input_date}' as day_date
        from {p_edw_schema}.mod_sku_day_return_result a
        inner join {p_edw_schema}.dim_target_store s on a.send_store_id = s.store_id
        inner join {p_edw_schema}.dim_target_store h on a.receive_store_id = h.store_id
        inner join {p_edw_schema}.dim_product_sku c on a.product_id = c.product_id and a.color_id = c.color_id
            and a.size_id = c.size_id
        where a.dec_day_date = date_add('{p_input_date}',1) 
            --and a.etl_time = '2018-09-17 18:57:03'           
    '''

    sql_return_2 = '''
        select product_code,color_code,size_code,send_org_code,receive_org_code
            ,concat(size_name,'+',suggest_return_qty,'+',size_code) as suggest_return_qty
            --,suggest_return_qty as suggest_return_qty_int
            ,day_date
        from return_1
    '''

    sql_return_3 = '''
        select product_code,color_code,send_org_code,receive_org_code,day_date
            ,concat_ws(',',collect_set(suggest_return_qty)) as suggest_return_qty
            --,sum(suggest_return_qty_int) as suggest_return_qty_int
        from return_2
        group by product_code,color_code,send_org_code,receive_org_code,day_date
    '''

    sql_after_ra_1 = '''
        select * from end_stock_1
        union ALL 
        select product_code,color_code,size_code
            ,receive_org_code as store_code
            ,size_name
            ,suggest_replenish_qty as stock_qty
            ,day_date
        from replenish_1
        union all
        select product_code,color_code,size_code
            ,send_org_code as store_code
            ,size_name
            ,-suggest_allot_qty as stock_qty
            ,day_date
        from allot_1
        union all
        select product_code,color_code,size_code
            ,receive_org_code as store_code
            ,size_name
            ,suggest_allot_qty as stock_qty
            ,day_date
        from allot_1
        union all
        select product_code,color_code,size_code
            ,store_code,size_name
            ,road_stock_qty,day_date
        from road_stock_1
        union all 
        select product_code,color_code,size_code
            ,send_org_code as store_code
            ,size_name
            ,-suggest_return_qty as stock_qty
            ,day_date
        from return_1
    '''

    sql_after_ra_2 = '''
        select product_code,color_code,size_code,store_code
            ,min(size_name) as size_name
            ,sum(stock_qty) as replenish_allot_after_qty
            ,day_date
        from after_ra_1
        group by product_code,color_code,size_code,store_code,day_date
    '''
    sql_after_ra_3 = '''
        select product_code,color_code,size_code,store_code            --sku、门店
            ,concat(size_name,'+',replenish_allot_after_qty,'+',size_code) as replenish_allot_after_qty        --补调后库存
            ,day_date            
        from after_ra_2
    '''

    sql_after_ra_4 = '''
        select product_code,color_code--,size_code        --sku
            ,store_code,day_date                        --门店、日期
             ,concat_ws(',',collect_set(replenish_allot_after_qty)) as replenish_allot_after_qty   --补调后库存
        from after_ra_3
        group by product_code,color_code,store_code,day_date
    '''

    sql_on_order_1 = '''
        select b.product_code,b.color_code,b.size_code            --sku
            ,a.org_code as store_code                             --门店
            ,b.size_name
            ,a.on_order_qty                                      --在单库存数量
            ,'{p_input_date}' as day_date
        from {p_edw_schema}.mid_sku_org_day_road_stock_peacebird a
        inner join (
            select * from {p_edw_schema}.dim_target_store where is_store='Y'
        ) s 
            on a.org_code = s.store_code
        inner join {p_edw_schema}.dim_product_sku b 
            on a.product_id = b.product_id 
            and a.color_id = b.color_id
            and a.size_id = b.size_id
        where day_date = '{p_input_date}' 
    '''

    sql_on_order_2 = '''
        select product_code,color_code,store_code,size_code,day_date
            ,concat(size_name,'+',on_order_qty,'+',size_code) as on_order_qty
        from on_order_1
    '''

    sql_on_order_3 = '''
        select product_code,color_code,store_code,day_date
            ,concat_ws(',',collect_set(on_order_qty)) as on_order_qty
        from on_order_2
        group by product_code,color_code,store_code,day_date
    '''

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_skc_store_ra_model partition(day_date)
        select after_ra_4.product_code                      --产品编码
            , p.product_name                             --产品名称
            , pc.color_code                              --颜色编码
            , pc.color_name                              --颜色名称
            , p.year_id as year                          --年份
            , p.quarter_id as quarter                    --季节
            , p.band                                     --波段
            , p.tiny_class                               --小类
            , b.prod_class as product_class              --产品分类                
            , c.org_code as send_org_code         --调出门店编码
            , c.org_name as send_org_name                --调出门店名称    
            , c.org_longcode as send_org_long_code       --调出门店唱编码
            , end_stock_3.stock_qty                           --库存数量
            , coalesce(road_stock_3.road_stock_qty,0) as road_stock_qty --在途库存,没有补0
            , coalesce(sales_3.week_sales_qty,0) as week_sales_qty --近一周销量，没有补零
            , coalesce(sales_3.week_sales_qty_int,0) as week_sales_qty_int --近一周销量，没有补零
            , coalesce(sales_3.total_sales_qty,0) as total_sales_qty --累计销量
            , coalesce(sales_3.total_sales_qty_str,0) as total_sales_qty_str --累计销量
            , on_order_3.on_order_qty as on_order_qty                        --在单数量
            , target_stock_3.target_inventory_qty                 --目标库存,没给就是null
            , replenish_3.suggest_replenish_qty                --建议补货量，没有就是null
            , replenish_3.suggest_replenish_qty_int
            , allot_3.suggest_allot_qty                    --建议调出量，没有就是null
            , allot_3.suggest_allot_qty_int
            , return_3.suggest_return_qty                --建议返仓量，没有就是null
            , null as replenish_allot_qty            --skc的建议补调量,没有就是null
            , after_ra_4.replenish_allot_after_qty as replenish_allot_after_stock           --补调后库存（加在途）
            , hhh.org_code as receive_org_code            --调入门店编码,没有就为null
            , hhh.org_name as receive_org_name              --调入门店名称
            , hhh.org_longcode as receive_org_long_code     --调入门店长编码
            , k.org_code as city_code                     --城市编码
            , k.org_longcode as city_long_code             --城市长编码
            , null as suggest_target                      --策略目的
            , null as modify_reason                       --手工修改原因
            , store.zone_id
            , store.zone_name
            , current_timestamp as etl_time               --etl时间
            , '{p_input_date}' as day_date       --日期
        from after_ra_4  
        inner join {p_edw_schema}.dim_product p 
            on after_ra_4.product_code = p.product_code
        inner join {p_edw_schema}.dim_product_skc pc 
            on after_ra_4.product_code = pc.product_code 
            and after_ra_4.color_code = pc.color_code
        left join (
            select * from {p_edw_schema}.mod_skc_week_classify where cla_week_date = '{p_input_date_mon}'
        ) as b
            on p.product_id = b.product_id 
            and pc.color_id = b.color_id
        inner join {p_edw_schema}.dim_stockorg c 
            on after_ra_4.store_code = c.org_code
        inner join {p_edw_schema}.dim_target_store as store
            on c.org_code=store.store_code
        left join road_stock_3 
            on after_ra_4.product_code = road_stock_3.product_code 
            and after_ra_4.color_code = road_stock_3.color_code 
            and after_ra_4.store_code = road_stock_3.store_code
        left join sales_3 
            on after_ra_4.product_code = sales_3.product_code 
            and after_ra_4.color_code = sales_3.color_code 
            and after_ra_4.store_code = sales_3.store_code
        left join target_stock_3 
            on after_ra_4.product_code = target_stock_3.product_code 
            and after_ra_4.color_code = target_stock_3.color_code 
            and after_ra_4.store_code = target_stock_3.store_code
        left join replenish_3 
            on after_ra_4.product_code = replenish_3.product_code 
            and after_ra_4.color_code = replenish_3.color_code 
            and after_ra_4.store_code = replenish_3.receive_org_code
        left join allot_3 
            on after_ra_4.product_code = allot_3.product_code 
            and after_ra_4.color_code = allot_3.color_code 
            and after_ra_4.store_code = allot_3.send_org_code
        left join return_3
            on after_ra_4.product_code = return_3.product_code 
            and after_ra_4.color_code = return_3.color_code 
            and after_ra_4.store_code = return_3.send_org_code
        left join {p_edw_schema}.dim_stockorg hhh 
            on allot_3.receive_org_code = hhh.org_code
        left join end_stock_3 
            on end_stock_3.product_code = after_ra_4.product_code 
            and end_stock_3.color_code = after_ra_4.color_code 
            and end_stock_3.store_code = after_ra_4.store_code
        inner join {p_edw_schema}.dim_stockorg k 
            on c.parent_id = k.org_id
        left join on_order_3
            on after_ra_4.product_code = on_order_3.product_code 
            and after_ra_4.color_code = on_order_3.color_code    
            and after_ra_4.store_code = on_order_3.store_code
    '''

    spark.create_temp_table(sql_end_stock_1, 'end_stock_1')
    spark.create_temp_table(sql_end_stock_2, 'end_stock_2')
    spark.create_temp_table(sql_end_stock_3, 'end_stock_3')
    spark.create_temp_table(sql_road_stock_1, 'road_stock_1')
    spark.create_temp_table(sql_road_stock_2, 'road_stock_2')
    spark.create_temp_table(sql_road_stock_3, 'road_stock_3')
    spark.create_temp_table(sql_sales_1, 'sales_1')
    spark.create_temp_table(sql_sales_2, 'sales_2')
    spark.create_temp_table(sql_sales_3, 'sales_3')
    spark.create_temp_table(sql_target_stock_1, 'target_stock_1')
    spark.create_temp_table(sql_target_stock_2, 'target_stock_2')
    spark.create_temp_table(sql_target_stock_3, 'target_stock_3')
    spark.create_temp_table(sql_replenish_1, 'replenish_1')
    spark.create_temp_table(sql_replenish_2, 'replenish_2')
    spark.create_temp_table(sql_replenish_3, 'replenish_3')
    spark.create_temp_table(sql_allot_1, 'allot_1')
    spark.create_temp_table(sql_allot_2, 'allot_2')
    spark.create_temp_table(sql_allot_3, 'allot_3')
    spark.create_temp_table(sql_return_1, 'return_1')
    spark.create_temp_table(sql_return_2, 'return_2')
    spark.create_temp_table(sql_return_3, 'return_3')
    spark.create_temp_table(sql_after_ra_1, 'after_ra_1')
    spark.create_temp_table(sql_after_ra_2, 'after_ra_2')
    spark.create_temp_table(sql_after_ra_3, 'after_ra_3')
    spark.create_temp_table(sql_after_ra_4, 'after_ra_4')
    spark.create_temp_table(sql_on_order_1, 'on_order_1')
    spark.create_temp_table(sql_on_order_2, 'on_order_2')
    spark.create_temp_table(sql_on_order_3, 'on_order_3')
    spark.execute_sql(sql_insert)
    spark.drop_temp_table('end_stock_1')
    spark.drop_temp_table('end_stock_2')
    spark.drop_temp_table('end_stock_3')
    spark.drop_temp_table('road_stock_1')
    spark.drop_temp_table('road_stock_2')
    spark.drop_temp_table('road_stock_3')
    spark.drop_temp_table('sales_1')
    spark.drop_temp_table('sales_2')
    spark.drop_temp_table('sales_3')
    spark.drop_temp_table('target_stock_1')
    spark.drop_temp_table('target_stock_2')
    spark.drop_temp_table('target_stock_3')
    spark.drop_temp_table('replenish_1')
    spark.drop_temp_table('replenish_2')
    spark.drop_temp_table('replenish_3')
    spark.drop_temp_table('allot_1')
    spark.drop_temp_table('allot_2')
    spark.drop_temp_table('allot_3')
    spark.drop_temp_table('return_1')
    spark.drop_temp_table('return_2')
    spark.drop_temp_table('return_3')
    spark.drop_temp_table('after_ra_1')
    spark.drop_temp_table('after_ra_2')
    spark.drop_temp_table('after_ra_3')
    spark.drop_temp_table('after_ra_4')
    spark.drop_temp_table('on_order_1')
    spark.drop_temp_table('on_order_2')
    spark.drop_temp_table('on_order_3')
