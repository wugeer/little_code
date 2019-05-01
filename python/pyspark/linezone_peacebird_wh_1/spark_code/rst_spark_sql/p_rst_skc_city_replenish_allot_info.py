# -*- coding: utf-8 -*-

# --------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_skc_city_replenish_allot_info
# Author: lh
# Date: 2018/9/26 10:21
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit

if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)
    
    #计算补调量，不包含返仓
    #1、构建临时表 skc 调出城市 调入城市 补调量 日期
    sql_replenish_allot = '''
        --1.1 计算补货量
        select 
            a.product_id
            ,a.color_id											--skc
            --,ds1.store_code as send_org_code					--调出组织编码
            ,ds1.city_long_code as send_city_long_code			--补货是仓库往门店补货，将调出城市为null
            --,ds.store_code as receive_org_code					--调入组织编码
            ,ds.city_long_code as receive_city_long_code		--调入长城市编码
            ,sum(a.send_qty) as replenish_allot_qty    			--发货数量（补货量）
            ,'{p_input_date}' as day_date
        from {p_edw_schema}.mod_sku_day_replenish a
        inner join {p_edw_schema}.dim_target_store ds1
            on a.send_org_id = ds1.store_id
        inner join {p_edw_schema}.dim_target_store ds
            on a.receive_store_id = ds.store_id
        where a.dec_day_date = date_add('{p_input_date}',1)
        group by a.product_id,a.color_id,ds1.city_long_code,ds.city_long_code
        
        union all 
        
        --1.2计算调拨量
        select 
            a.product_id
            ,a.color_id									--skc	
            --,ds1.store_code as send_org_code					--调出组织编码	
            ,ds1.city_long_code as send_city_long_code				--调入城市长编码
            --,ds.store_code as receive_org_code					--调入组织编码
            ,ds.city_long_code as receive_city_long_code				--调入城市编码
            ,sum(a.send_qty) as replenish_allot_qty    		--发货数量（调拨量）
            ,'{p_input_date}' as day_date
        from {p_edw_schema}.mod_sku_day_allot a
        inner join {p_edw_schema}.dim_target_store ds1
            on a.send_store_id = ds1.store_id
        inner join {p_edw_schema}.dim_target_store ds
            on a.receive_store_id = ds.store_id
        where a.dec_day_date = date_add('{p_input_date}',1)
        group by a.product_id,a.color_id,ds1.city_long_code,ds.city_long_code
    '''
    
    #--在通过城市长编码 city_long_code 关联 {p_edw_schema}.dim_store  取 区域用户id  zone_id 时会出现数据重复,首先对这两个字段进行去重
    sql_dim_store = '''
        select
            city_long_code
            , zone_id
        from {p_edw_schema}.dim_target_store
        group by city_long_code, zone_id
    '''
    
    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_skc_city_replenish_allot_info partition(day_date) 
        select 
            dp.product_code                         --产品编码
            ,dp.product_name                             --产品名称
            ,dps.color_code                              --颜色编码
            ,dps.color_name                              --颜色名称
            ,dp.year_id as year                          --年份
            ,dp.quarter_id as quarter                    --季节
            ,dp.band                                     --波段
            ,dp.tiny_class                               --小类
            ,b.prod_class as product_class               --产品分类                   
            ,tra.send_city_long_code			         --调出门店长编码
            ,(case
                when tra.send_city_long_code is not null then ds1.zone_id
                else cast(null as string)
                end
             )as send_zone_id							--调出区域用户id
            ,tra.replenish_allot_qty            		 --skc的建议补货量
            ,tra.receive_city_long_code			         --调出门店长编码
            ,ds.zone_id as receive_zone_id
            --,'{p_input_date}' as day_date       --日期
            ,current_timestamp as etl_time               --etl时间
            ,'{p_input_date}' as day_date       --日期
        from tmp_replenish_allot tra
        inner join {p_edw_schema}.dim_product dp
            on tra.product_id = dp.product_id
        inner join {p_edw_schema}.dim_product_skc dps 
            on tra.product_id = dps.product_id 
            and tra.color_id = dps.color_id
        left join (
            select * from {p_edw_schema}.mod_skc_week_classify where cla_week_date = '{p_input_date_mon}'
        ) as b
            on tra.product_id = b.product_id 
            and tra.color_id = b.color_id
        --这里不能使用inner join 因为 send_city_long_code 中部分值为空， inner join会筛选掉部分的数据
        --inner join tmp_dim_store as ds1                             -- 取 调出区域用户id
        left join tmp_dim_store as ds1
            on tra.send_city_long_code = ds1.city_long_code
        inner join tmp_dim_store as ds                              --取调出门店的 区域用户id   zone_id
            on tra.receive_city_long_code = ds.city_long_code
    '''
    
    spark.create_temp_table(sql_replenish_allot, 'tmp_replenish_allot')
    spark.create_temp_table(sql_dim_store, 'tmp_dim_store')
    
    spark.execute_sql(sql_insert)
    
    spark.drop_temp_table('tmp_replenish_allot')
    spark.drop_temp_table('tmp_dim_store')
