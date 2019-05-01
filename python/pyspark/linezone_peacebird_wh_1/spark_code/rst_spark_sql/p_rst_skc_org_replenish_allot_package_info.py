# -*- coding: utf-8 -*-

# --------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_skc_city_replenish_allot_info
# Author: lh
# Date: 2018/9/26 10:34
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit

if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)
    
    #--计算包裹数，不包含返仓
    #--1、构建临时表 skc 调出门店 调出城市 调入门店 调入城市 日期
    #--计算包裹数只能到skc门店的原因 见 对应的hive sql文件
    sql_replenish_allot = '''
        select 
            a.product_id
            ,a.color_id									--skc
            ,ds1.store_code as send_org_code			--调出组织编码
            ,ds.store_code as receive_org_code			--调入组织编码
            ,'{p_input_date}' as day_date
        from edw.mod_sku_day_replenish a
        inner join edw.dim_target_store ds1
            on a.send_org_id = ds1.store_id
        inner join edw.dim_target_store ds
            on a.receive_store_id = ds.store_id
        where a.dec_day_date = date_add('{p_input_date}',1)
        group by a.product_id,a.color_id,ds1.store_code,ds.store_code
        
        union all 
        
        select 
            a.product_id
            ,a.color_id									--skc			
            ,ds1.store_code as send_org_code			--调出组织编码
            ,ds.store_code as receive_org_code			--调入组织编码
            ,'{p_input_date}' as day_date
        from edw.mod_sku_day_allot a
        inner join edw.dim_target_store ds1
            on a.send_store_id = ds1.store_id
        inner join edw.dim_target_store ds
            on a.receive_store_id = ds.store_id
        where a.dec_day_date = date_add('{p_input_date}',1)
        group by a.product_id,a.color_id,ds1.store_code,ds.store_code
    '''
    
    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_skc_org_replenish_allot_package_info partition(day_date) 
        select 
            dp.product_code                         	        --产品编码
            ,dp.product_name                                     --产品名称
            ,dps.color_code                                      --颜色编码
            ,dps.color_name                                      --颜色名称
            ,dp.year_id as year                                  --年份
            ,dp.quarter_id as quarter                            --季节
            ,dp.band                                             --波段
            ,dp.tiny_class                                       --小类
            ,b.prod_class as product_class                       --产品分类
            ,tra.send_org_code		    				         --调出组织编码
            ,ds1.city_long_code as send_city_long_code			 --调出城市长编码
            ,ds1.zone_id as send_zone_id						 --调出区域用户id
            ,tra.receive_org_code							     --调入组织编码
            ,ds.city_long_code as receive_city_long_code	     --调入门店长编码
            ,ds.zone_id as receive_zone_id						 --调入区域用户id
            --,'{p_input_date}' as day_date       --日期
            ,current_timestamp as etl_time               --etl时间
            ,'{p_input_date}' as day_date       --日期
        from tmp_replenish_allot tra
        inner join edw.dim_product dp
            on tra.product_id = dp.product_id
        inner join edw.dim_product_skc dps 
            on tra.product_id = dps.product_id 
            and tra.color_id = dps.color_id
        left join (
            select * from {p_edw_schema}.mod_skc_week_classify where cla_week_date = '{p_input_date_mon}'
        ) as b
            on tra.product_id = b.product_id 
            and tra.color_id = b.color_id
        inner join edw.dim_target_store as ds1
            on tra.send_org_code=ds1.store_code
        inner join edw.dim_target_store as ds
            on tra.receive_org_code=ds.store_code
    '''
    
    spark.create_temp_table(sql_replenish_allot, 'tmp_replenish_allot')
    
    spark.execute_sql(sql_insert)
    
    spark.drop_temp_table('tmp_replenish_allot')
