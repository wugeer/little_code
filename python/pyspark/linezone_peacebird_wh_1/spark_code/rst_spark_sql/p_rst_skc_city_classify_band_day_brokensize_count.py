# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_skc_city_classify_band_day_brokensize_count
# Author: lh
# Date: 2018/10/10 12:28
# ----------------------------------------------------------------------------
 

import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    # 补调前 断码门店 及总门店数
    sql_before_rep_allot = """
        select a.product_code
            , a.color_code
            , sum(case a.available_is_fullsize when 'N' then 1 else 0 end) as before_rep_allot_brokensize_count
            , count(1) as before_rep_allot_skc_count
            , c.org_longcode
        from {p_dm_schema}.dm_skc_store_day_available_fullsize a
        inner join {p_edw_schema}.dim_stockorg b on a.store_code = b.org_code              --门店
        inner join {p_edw_schema}.dim_stockorg c on b.parent_id = c.org_id     --城市
        where a.day_date = '{p_input_date}'
        group by a.product_code,color_code,c.org_longcode
    """

    # 补调后 断码门店 及总门店数
    sql_after_rep_allot = """
        select a.product_code
            , a.color_code
            , sum(case a.is_fullsize when 'N' then 1 else 0 end) as after_rep_allot_brokensize_count
            , count(1) as after_rep_allot_skc_count
            , c.org_longcode
        from {p_dm_schema}.dm_skc_store_day_after_replenish_allot_fullsize a
        inner join {p_edw_schema}.dim_stockorg b on a.store_code = b.org_code              --门店
        inner join {p_edw_schema}.dim_stockorg c on b.parent_id = c.org_id     --城市
        where a.day_date = '{p_input_date}'
        group by a.product_code,color_code,c.org_longcode
    """
    
    #--在通过城市长编码 city_long_code 关联 {p_edw_schema}.dim_store  取 区域用户id  zone_id 时会出现数据重复,首先对这两个字段进行去重
    sql_dim_store = '''
        select
            city_long_code
            , zone_id
        from {p_edw_schema}.dim_store
        group by city_long_code, zone_id
    '''

    sql_insert = """
        insert overwrite table {p_rst_schema}.rst_skc_city_classify_band_day_brokensize_count
        partition(day_date)
        select 
            dp.product_code                         --产品编码
            , dp.product_name                             --产品名称
            , dps.color_code                              --颜色编码
            , dps.color_name                              --颜色名称
            , dp.year_id as year                          --年份
            , dp.quarter_id as quarter                    --季节
            , dp.band                                     --波段
            , dp.tiny_class                               --小类
            , b.prod_class as product_class               --产品分类 
            , ara.org_longcode as city_long_code
            , ds.zone_id as receive_zone_id
            , bra.before_rep_allot_skc_count
            , bra.before_rep_allot_brokensize_count
            , ara.after_rep_allot_skc_count
            , ara.after_rep_allot_brokensize_count
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from after_rep_allot ara 
        inner join {p_edw_schema}.dim_product dp
            on ara.product_code = dp.product_code
        inner join {p_edw_schema}.dim_product_skc dps 
            on ara.product_code = dps.product_code 
            and ara.color_code = dps.color_code
        left join (
            select * from {p_edw_schema}.mod_skc_week_classify where cla_week_date = '{p_input_date_mon}'
        ) as b
            on dps.product_id = b.product_id 
            and dps.color_id = b.color_id
        inner join tmp_dim_store as ds                              --取调出门店的 区域用户id   zone_id
            on ara.org_longcode = ds.city_long_code
        left join before_rep_allot bra 
            on bra.org_longcode = ara.org_longcode and bra.product_code = ara.product_code 
            and bra.color_code = ara.color_code
    """    
        
    spark.create_temp_table(sql_before_rep_allot, 'before_rep_allot')
    spark.create_temp_table(sql_after_rep_allot, 'after_rep_allot')
    spark.create_temp_table(sql_dim_store, 'tmp_dim_store')
    
    spark.execute_sql(sql_insert)
    

    spark.drop_temp_table('before_rep_allot')
    spark.drop_temp_table('after_rep_allot')
    spark.drop_temp_table('tmp_dim_store')