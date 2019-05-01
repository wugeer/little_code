# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_dm_sku_org_week_replenish_allot_return_comp
# Author: lh
# Date: 2018/10/08 12:28
# change: 2019年1月17日15:08:50  计算匹配度只考虑调拨，不考虑返仓和补货
# ----------------------------------------------------------------------------

# 判断是否等于星期天
# if datetime.strptime("2018-09-29","%Y-%m-%d").weekday() == 6:  

import os
from datetime import datetime
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)
    
    #判断传入日期是否等于星期天
    if datetime.strptime(spark.params_dict['p_input_date'],'%Y-%m-%d').weekday() == 6:
        #print('星期天')
        #sku 补货 调拨 返仓 情况对比
        
        #取 自营 正常门店 id 
        sql_tmp_dim_store = '''
            select
                store_id
            from {p_edw_schema}.dim_target_store 
            where is_store='Y' and status='正常'
        '''
        
        #取上周的目标产品数据
        sql_tmp_target_product = '''
            select 
                product_id
                , day_date
            from {p_edw_schema}.dim_target_product
            where day_date >= '{p_input_date_mon}' and day_date <= '{p_input_date}'
            group by day_date, product_id               
        '''
        
        #取上周的库存数据
        sql_tmp_stock = '''
            SELECT 
                product_id
                , color_id
                , size_id
                , send_org_id
                , receive_org_id
                , mid_org_id
                , movetype_code
                , send_qty
                , send_amt
                , send_date
            FROM {p_edw_schema}.fct_stock
            where send_date >= '{p_input_date_mon}' and send_date <= '{p_input_date}'
                and send_qty <> 0   -- 排除 发送量 等于 0 的情况
        '''        

        #2、sku 调拨 情况对比
        #2.1 sku 模型 调拨 情况
        sql_tmp_ai_allot = '''
            select a.product_id
                , a.color_id
                , a.size_id
                , a.send_store_id as send_org_id
                , a.receive_store_id as receive_org_id
                , min(a.date_send) as ai_send_date
                , sum(a.send_qty) as ai_allot_qty
            from {p_edw_schema}.mod_sku_day_allot_old a
            inner join tmp_dim_store ds
                on a.send_store_id = ds.store_id
            inner join tmp_dim_store b
                on a.receive_store_id = b.store_id 
            --where a.date_send = '{p_input_date_mon}'      --模型调拨一周一次
            where a.date_send >= '{p_input_date_mon}' and a.date_send <= '{p_input_date}' --取上周一到上周日的数据
            group by a.product_id, a.color_id, a.size_id, a.send_store_id, a.receive_store_id
        '''
        
        #2.1 sku 实际 调拨 情况（fct_stock）
        sql_tmp_peacebird_allot = '''
            select a.product_id
                , a.color_id
                , a.size_id
                , a.send_org_id
                , a.receive_org_id
                , min(a.send_date) as peacebird_send_date
                , sum(a.send_qty) as peacebird_allot_qty
            from tmp_stock a
            inner join tmp_dim_store ds 
                on a.send_org_id = ds.store_id        
            inner join tmp_dim_store b 
                on a.receive_org_id = b.store_id
            inner join tmp_target_product c 
              on a.send_date = c.day_date 
                and a.product_id = c.product_id
            group by a.product_id, a.color_id, a.size_id, a.send_org_id, a.receive_org_id
        '''  
        
        
        sql_tmp_after_rar = '''
            select
                coalesce(tpa.product_id , taa.product_id) as product_id
                , coalesce(tpa.color_id , taa.color_id) as color_id
                , coalesce(tpa.size_id , taa.size_id) as size_id
                , coalesce(tpa.send_org_id, taa.send_org_id) as send_org_id
                , coalesce(tpa.receive_org_id, taa.receive_org_id) as receive_org_id
                , coalesce(taa.ai_allot_qty, 0) as ai_trans_qty                  -- 调拨  模型转移量
                , taa.ai_send_date
                , coalesce(tpa.peacebird_allot_qty, 0) as peacebird_trans_qty    -- 调拨  实际转移量
                , tpa.peacebird_send_date
                , 'all' as flag                                     -- 调拨
                , '{p_input_date_mon}'  as week_date                -- 上周一的日期作为对比日期           
            from tmp_peacebird_allot tpa                            -- 分母为  人工决策的行数（人工发货数量大于 0）
            full join tmp_ai_allot taa
                on tpa.product_id = taa.product_id
                    and tpa.color_id = taa.color_id
                    and tpa.size_id = taa.size_id
                    and tpa.send_org_id = taa.send_org_id
                    and tpa.receive_org_id = taa.receive_org_id                     
        '''
        
        sql_insert = '''
            insert overwrite table {p_dm_schema}.dm_sku_org_week_replenish_allot_return_comp partition(week_date)
            select
                dps.product_code
                , dps.color_code
                , dps.size_code
                --, a.send_org_id
                , ds.store_code as send_org_code      --    调入组织编码
                --, a.receive_org_id
                , ds1.store_code as receive_org_code   --   调出组织编码
                , a.ai_trans_qty                       --   模型转移量
                , a.ai_send_date
                , a.peacebird_trans_qty                --   实际转移量
                , a.peacebird_send_date
                , a.flag 
                , current_timestamp as etl_time      -- 
                , a.week_date                -- 上周一的日期作为对比日期
            from tmp_after_rar a
            inner join {p_edw_schema}.dim_product_sku dps
               on a.product_id = dps.product_id
                    and a.color_id = dps.color_id
                    and a.size_id = dps.size_id
            inner join {p_edw_schema}.dim_target_store ds 
                on a.send_org_id =  ds.store_id
            inner join {p_edw_schema}.dim_target_store ds1 
                on a.receive_org_id =  ds1.store_id
        '''   
        
        spark.create_temp_table(sql_tmp_dim_store, 'tmp_dim_store')
        spark.create_temp_table(sql_tmp_target_product, 'tmp_target_product')
        spark.create_temp_table(sql_tmp_stock, 'tmp_stock')
        spark.create_temp_table(sql_tmp_ai_allot, 'tmp_ai_allot')
        spark.create_temp_table(sql_tmp_peacebird_allot, 'tmp_peacebird_allot')
        spark.create_temp_table(sql_tmp_after_rar, 'tmp_after_rar')
        
        spark.execute_sql(sql_insert)
        
        spark.drop_temp_table('tmp_after_rar')
        spark.drop_temp_table('tmp_peacebird_allot')
        spark.drop_temp_table('tmp_ai_allot')
        spark.drop_temp_table('tmp_stock')
        spark.drop_temp_table('tmp_target_product')
        spark.drop_temp_table('tmp_dim_store')   