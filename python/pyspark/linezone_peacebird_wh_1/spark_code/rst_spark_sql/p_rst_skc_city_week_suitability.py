# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_skc_city_week_suitability
# Author: lh
# Date: 2018/10/10 12:28
# ----------------------------------------------------------------------------

# 判断是否等于星期天
#from datetime import datetime
# if datetime.strptime("2018-09-29","%Y-%m-%d").weekday() == 6:  

import os
from utils.tools import SparkInit

from datetime import datetime


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)
    
    #判断传入日期是否等于星期天
    if datetime.strptime(spark.params_dict['p_input_date'],'%Y-%m-%d').weekday() == 6:
        
        # 取周一的数据
        sql_tmp_rar_comp = '''
            select 
                product_code
                , color_code
                , size_code
                , send_org_code
                , receive_org_code
                , ai_trans_qty
                , ai_send_date
                , peacebird_trans_qty
                , peacebird_send_date
            from {p_dm_schema}.dm_sku_org_week_replenish_allot_return_comp
            where week_date = '{p_input_date_mon}'   
        '''
        
        #sku 发货匹配情况
        sql_tmp_send_suit = '''
            select 
                product_code
                , color_code
                , size_code
                , send_org_code
                , (case 
                    when sum(ai_trans_qty) > 0 and sum(peacebird_trans_qty) > 0  then 1
                    else 0
                  end)
                    as send_suit_flag            --匹配为1， 不匹配为0
            from tmp_rar_comp
            --where peacebird_send_date is not null and peacebird_trans_qty >=0         -- 排除人工发货<0 的情况, 取实际决策记录
            group by product_code, color_code, size_code, send_org_code
            having min(peacebird_send_date) is not null   -- 分组后，取实际决策的记录  不能 在分组前进行记录的筛选
                and sum(peacebird_trans_qty) >= 0   -- 分组后 排除人工发货<0 的情况
        '''
        
        #sku 收货匹配情况
        sql_tmp_receive_suit = '''
            select 
                product_code
                , color_code
                , size_code
                , receive_org_code
                , case 
                    when sum(ai_trans_qty) > 0 and sum(peacebird_trans_qty) > 0  then 1
                    else 0
                  end 
                    as receive_suit_flag            --匹配为1， 不匹配为0
            from tmp_rar_comp
            --where peacebird_send_date is not null and peacebird_trans_qty >=0         -- 排除人工发货<0 的情况, 取实际决策记录
            group by product_code, color_code, size_code, receive_org_code
            having min(peacebird_send_date) is not null   -- 分组后，取实际决策的记录  不能 在分组前进行记录的筛选
                and sum(peacebird_trans_qty) >= 0   -- 分组后 排除人工收货<0 的情况
        '''
        
        #sku 综合匹配情况
        #1、sku 门店 - 门店  sku 门店 - CB11
        #2、sku CB37 - 门店
        sql_tmp_composite_suit = '''
            --sku 门店 - 门店  sku 门店 - CB11
            select 
                product_code
                , color_code
                , size_code
                , send_org_code                                 
                -- sql 中没有这种用法
                --, min(ai_trans_qty, peacebird_trans_qty)*1.0/max(ai_trans_qty, peacebird_trans_qty) as composite_suit_flag            --匹配标记在[0,1]之间
                , case
                    when ai_trans_qty <= peacebird_trans_qty then  ai_trans_qty/peacebird_trans_qty
                    else peacebird_trans_qty/ai_trans_qty
                  end
                    as composite_suit_flag
            from tmp_rar_comp
            where peacebird_send_date is not null and peacebird_trans_qty > 0   -- 只取 发货组织+收货组织+SKC+尺码+数量 都存在的情况              
        '''
                    
        sql_tmp_city_src_suit_1 = '''
        --# 年份 季节 城市 发货匹配情况
            select
                dp.year_id as year
                , dp.quarter_id as quarter
                , ds.city_code                    -- 城市编码
                , ds.city_long_code               -- 城市长编码
                , sum(tss.send_suit_flag) as send_suit_numerator  -- 发货匹配度分子
                , count(1) as send_suit_denominator               -- 发货匹配度分母
                , 0 as receive_suit_numerator                     -- 收货匹配度分子
                , 0 as receive_suit_denominator                   -- 收货匹配度分母
                , 0 as composite_suit_numerator                   -- 综合匹配度分子
                , 0 as composite_suit_denominator                 -- 综合匹配度分母
            from tmp_send_suit tss
            inner join {p_edw_schema}.dim_product dp    -- 取年份季节
                on tss.product_code = dp.product_code
            inner join {p_edw_schema}.dim_target_store ds      -- 取区域id / 城市编码
                on tss.send_org_code = ds.store_code
            group by dp.year_id, dp.quarter_id, ds.city_code, ds.city_long_code
        
            union all
        
         -- # 年份 季节 城市 收货匹配情况
            select
                dp.year_id as year
                , dp.quarter_id as quarter
                , ds.city_code                    -- 城市编码
                , ds.city_long_code               -- 城市长编码
                , 0 as send_suit_numerator                                  -- 发货匹配度分子
                , 0 as send_suit_denominator                                -- 发货匹配度分母
                , sum(tss.receive_suit_flag) as receive_suit_numerator      -- 收货匹配度分子
                , count(1) as receive_suit_denominator                      -- 收货匹配度分母
                , 0 as composite_suit_numerator                             -- 综合匹配度分子
                , 0 as composite_suit_denominator                           -- 综合匹配度分母
            from tmp_receive_suit tss
            inner join {p_edw_schema}.dim_product dp    -- 取年份季节
                on tss.product_code = dp.product_code
            inner join {p_edw_schema}.dim_store ds      -- 取区域id / 城市编码
                on tss.receive_org_code = ds.store_code
            group by dp.year_id, dp.quarter_id, ds.city_code, ds.city_long_code
        
            union all

        --# 年份 季节 城市 综合匹配情况
            select
                dp.year_id as year
                , dp.quarter_id as quarter
                , ds.city_code                    -- 城市编码
                , ds.city_long_code               -- 城市长编码
                , 0 as send_suit_numerator                                  -- 发货匹配度分子
                , 0 as send_suit_denominator                                -- 发货匹配度分母
                , 0 as receive_suit_numerator                               -- 收货匹配度分子
                , 0 as receive_suit_denominator                             -- 收货匹配度分母
                , sum(tcs.composite_suit_flag) as composite_suit_numerator  -- 综合匹配度分子
                , count(1) as composite_suit_denominator                    -- 综合匹配度分母
            from tmp_composite_suit tcs
            inner join {p_edw_schema}.dim_product dp    -- 取年份季节
                on tcs.product_code = dp.product_code
            inner join {p_edw_schema}.dim_store ds      -- 取区域id / 城市编码
                on tcs.send_org_code = ds.store_code
            group by dp.year_id, dp.quarter_id, ds.city_code, ds.city_long_code
        '''
        
        sql_tmp_city_src_suit_2 = '''
            select
                year
                , quarter
                , city_code
                , city_long_code
                , sum(send_suit_numerator) as send_suit_numerator                                -- 发货匹配度分子
                , sum(send_suit_denominator) as send_suit_denominator                            -- 发货匹配度分母
                , sum(receive_suit_numerator) as receive_suit_numerator                          -- 收货匹配度分子
                , sum(receive_suit_denominator) as receive_suit_denominator                      -- 收货匹配度分母
                , sum(composite_suit_numerator) as composite_suit_numerator                      -- 综合匹配度分子
                , sum(composite_suit_denominator) as composite_suit_denominator                  -- 综合匹配度分母
            from tmp_city_src_suit_1 
            group by year, quarter, city_code, city_long_code      
        '''
     
        #--在通过城市长编码 city_long_code 关联 {p_edw_schema}.dim_store  取 区域用户id  zone_id 时会出现数据重复,首先对这两个字段进行去重
        sql_tmp_store = '''
            select
                city_long_code
                , zone_id
                , zone_name
            from {p_edw_schema}.dim_target_store
            group by city_long_code, zone_id, zone_name
        '''

        
        sql_insert = '''
            insert overwrite table {p_rst_schema}.rst_skc_city_week_suitability partition(week_date)
            select
                tcss2.year
                , tcss2.quarter
                , ds1.org_code as region_code               -- 大区编码
                , ds1.org_longcode as region_long_code      -- 大区长编码
                , ts.zone_id as zone_long_code
                , ts.zone_name as zone_name
                , tcss2.city_code
                , tcss2.city_long_code
                , tcss2.send_suit_numerator                     -- 发货匹配度分子
                , tcss2.send_suit_denominator                       -- 发货匹配度分母
                , tcss2.receive_suit_numerator                  -- 收货匹配度分子
                , tcss2.receive_suit_denominator                --   收货匹配度分母
                , tcss2.composite_suit_numerator                -- 综合匹配度分子
                , tcss2.composite_suit_denominator              -- 综合匹配度分母
                , current_timestamp as etl_time      
                , '{p_input_date_mon}' as week_date                -- 上周一的日期作为对比日期
            from tmp_city_src_suit_2 tcss2
            inner join {p_edw_schema}.dim_stockorg  ds
                on tcss2.city_long_code = ds.org_longcode
            inner join {p_edw_schema}.dim_stockorg ds1
                on ds.parent_id = ds1.org_id
            inner join tmp_store ts
                on tcss2.city_long_code = ts.city_long_code
        '''
        
        spark.create_temp_table(sql_tmp_rar_comp, 'tmp_rar_comp')
        spark.create_temp_table(sql_tmp_send_suit, 'tmp_send_suit')
        spark.create_temp_table(sql_tmp_receive_suit, 'tmp_receive_suit')
        spark.create_temp_table(sql_tmp_composite_suit, 'tmp_composite_suit')
        spark.create_temp_table(sql_tmp_city_src_suit_1, 'tmp_city_src_suit_1')
        spark.create_temp_table(sql_tmp_city_src_suit_2, 'tmp_city_src_suit_2')
        spark.create_temp_table(sql_tmp_store, 'tmp_store')
        
        spark.execute_sql(sql_insert)
        
        spark.drop_temp_table('tmp_store')
        spark.drop_temp_table('tmp_city_src_suit_2')
        spark.drop_temp_table('tmp_city_src_suit_1')
        spark.drop_temp_table('tmp_composite_suit')
        spark.drop_temp_table('tmp_receive_suit')
        spark.drop_temp_table('tmp_send_suit')
        spark.drop_temp_table('tmp_rar_comp')  