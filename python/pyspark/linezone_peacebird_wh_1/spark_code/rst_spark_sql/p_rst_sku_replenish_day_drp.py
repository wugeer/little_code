# -*- coding: utf-8 -*-

# --------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_sku_replenish_day_drp
# Author: liuhai
# Date: 2018/11/05 14:38
# modified: 2018年11月16日16:39:45  增加门店调入量的限制
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit

if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    #取补货数据
#    sql_replenish = '''
#        select 
#            product_id
#            , color_id
#            , size_id
#            , send_org_id
#            , receive_store_id as receive_org_id
#            , send_qty
#            , from_unixtime(unix_timestamp(dec_day_date, 'yyyy-mm-dd'), 'yyyymmdd') as billdate
#        from {p_edw_schema}.mod_sku_day_replenish
#        where dec_day_date = date_add('{p_input_date}',1) 
#    '''
    
    # 取过滤后的目标库存
    sql_target_peacebird = '''
        SELECT 
            org_id, product_id, color_id, size_id , stock_qty, active
        FROM edw.mid_day_target_stock_peacebird
        where stock_date = '{p_input_date}' and stock_qty > 0 and active = 'Y'
        group by org_id, product_id, color_id, size_id , stock_qty, active
        --having sum(stock_qty) > 0    -- 取单个门店，单个skc 的库存数量大于0 的单款 单 skc    
    '''

    # 取当天的门店转移状态
    sql_store_trans_status = '''
        select 
            b.store_id
            , b.not_in
            , b.date_dec
        from edw.dim_store_trans_status b  
        where b.date_dec = date_add('{p_input_date}',1)
    '''

    # 取门店skc转移状态
    sql_store_skc_trans_status = '''
        select 
            b.store_id
            , b.product_id
            , b.color_id
            , b.not_in
            , b.date_dec
        from edw.dim_store_skc_trans_status b  
        where b.date_dec = date_add('{p_input_date}',1)
    '''
    
    # 门店状态为 not_in 的不补货
    sql_replenish_notin = '''
        select 
            a.product_id, a.color_id, a.size_id
            , a.send_org_id, a.receive_store_id
            , a.date_send, a.date_rec_pred
            , a.send_qty, a.dec_day_date
        from edw.mod_sku_day_replenish a
        -- where receive_store_id = '19335'
        left join store_trans_status b 
            on a.receive_store_id = b.store_id 
        left join store_skc_trans_status dss
            on a.receive_store_id = dss.store_id 
                and a.product_id = dss.product_id
                and a.color_id = dss.color_id
        where  (b.not_in <> '1' or b.not_in is null) 
            and a.product_id not in ('767304', '767385', '767410')    -- 这几个货号 所有的门店不补货 2018年12月14日14:58:29
            -- and a.receive_store_id <> '19473'  2018年11月27日03:56:04 没 过滤掉
            and dss.product_id is null
    '''
    
    sql_replenish = '''
        -- --主款的补货，要求目标库存状态为激活状态且前一天的目标库存不为0 
        select 
            a.product_id
            , a.color_id
            , a.size_id
            , a.send_org_id
            , a.receive_store_id as receive_org_id
            , (case
                    when a.send_qty > 5 then 5 
                    else a.send_qty 
                end) as send_qty
            --, a.send_qty 
            , (
                case when sjv.limit_qty is not null then sjv.limit_qty
                else 0
                end
                ) as limit_qty
            , from_unixtime(unix_timestamp(a.dec_day_date, 'yyyy-mm-dd'), 'yyyymmdd') as billdate
        from replenish_notin a 
        inner join target_peacebird b 
            on a.receive_store_id = b.org_id and a.product_id = b.product_id 
                and a.color_id = b.color_id and a.size_id = b.size_id
        left join peacebird.st_joinstore_view sjv
            on a.receive_store_id = sjv.c_store_id
        -- 临时处理模型暂时无法解决单个门店补货量过高的问题
        left join edw.dim_product_sku c 
            on a.product_id = c.product_id and a.color_id = c.color_id and a.size_id = c.size_id
        where c.sku_id  not in (select DISTINCT m_productalias2_id from  peacebird.st_sku_relate_view)
            --and a.receive_store_id <> '19473'           
        
        union all 

        -- 替代款的补货 
        select 
            a.product_id
            , a.color_id
            , a.size_id
            , a.send_org_id
            , a.receive_store_id as receive_org_id
            , (case
                    when a.send_qty > 5 then 5 
                    else a.send_qty 
                end) as send_qty
            --, a.send_qty
            , (
                case when sjv.limit_qty is not null then sjv.limit_qty
                else 0
                end
                ) as limit_qty      
            , from_unixtime(unix_timestamp(a.dec_day_date, 'yyyy-mm-dd'), 'yyyymmdd') as billdate
        from replenish_notin a
        left join edw.dim_product_sku b 
            on a.product_id = b.product_id and a.color_id = b.color_id and a.size_id = b.size_id
        inner join (select DISTINCT m_productalias_id,m_productalias2_id from  peacebird.st_sku_relate_view) d 
           on b.sku_id = d.m_productalias2_id
        --left join edw.dim_product_sku dps 
        --    on dps.sku_id = d.m_productalias_id        
        --inner join target_peacebird tp 
        --    on a.receive_store_id = tp.org_id 
        --        and dps.product_id = tp.product_id 
        --        and dps.color_id = tp.color_id 
        --        and dps.size_id = tp.size_id
        left join peacebird.st_joinstore_view sjv
            on a.receive_store_id = sjv.c_store_id
        -- 临时处理模型暂时无法解决单个门店补货量过高的问题
        --where a.dec_day_date = date_add('{p_input_date}',1) 
            --and a.receive_store_id <> '19473'
    '''
    # 将 send_org_code+receive_org_code+billdate 拼成所需的 refno 
    sql_insert_re_zy = '''
        insert overwrite table {p_rst_schema}.rst_sku_replenish_day_drp partition(billdate)
        select 
            ds.store_code as send_org_code
            , ds1.store_code as receive_org_code
            , concat(ds.store_code,'_',ds1.store_code,'_',ra.billdate) as refno
            , dpc.sku_code
            , ra.send_qty as qty
            , cast(ra.limit_qty as int) as receive_org_limt      -- 暂时将 调入组织的数量限制 定为 2 
            --, 0 as receive_org_limt     -- 暂时取 0 2018年11月27日20:41:48 以后会改过来的
            , sum(ra.send_qty) over(partition by ds.store_code, ds1.store_code)  as package_qty_sum                   -- 按调出组织和调入组织进行汇总             
            , current_timestamp as etl_time
            , ra.billdate
        from replenish ra 
        inner join {p_edw_schema}.dim_target_store ds 
            on ra.send_org_id = ds.store_id
        inner join {p_edw_schema}.dim_target_store ds1 
            on ra.receive_org_id = ds1.store_id
        inner join {p_edw_schema}.dim_product_sku dpc
            on ra.product_id = dpc.product_id 
                and ra.color_id = dpc.color_id
                and ra.size_id = dpc.size_id
    '''

    # 钱不够的加盟门店
    sql_tmp_store_jm = '''
        select
            c_store_id 
        from {p_rst_schema}.rst_customer_store_alarm_bak_drp
        --where billdate=from_unixtime(unix_timestamp(date_add('{p_input_date}',1), 'yyyy-mm-dd'), 'yyyymmdd')  -- 结果为 ****00** 不能写成这种方式
        where billdate=from_unixtime(unix_timestamp('{p_input_date_add_one_day}', 'yyyy-MM-dd'), 'yyyyMMdd')
            and amount_bill > 0 and is_bill = 'N'
    '''

    sql_insert_re_jm = '''
        insert overwrite table {p_rst_schema}.rst_sku_sales_slip_day_drp partition(billdate)
        select 
            ds.store_code as send_org_code
            , ds1.store_code as receive_org_code
            , concat(ds.store_code,'_',ds1.store_code,'_',ra.billdate) as refno
            , dpc.sku_code
            , ra.send_qty as qty
            , cast(ra.limit_qty as int) as receive_org_limt      -- 暂时将 调入组织的数量限制 定为 2 
            --, 0 as receive_org_limt     -- 暂时取 0 2018年11月27日20:41:48 以后会改过来的
            , sum(ra.send_qty) over(partition by ds.store_code, ds1.store_code)  as package_qty_sum                   -- 按调出组织和调入组织进行汇总             
            , current_timestamp as etl_time
            , ra.billdate
        from replenish ra
        left join tmp_store_jm tsj
            on ra.receive_org_id = tsj.c_store_id
        inner join {p_edw_schema}.dim_store ds 
            on ra.send_org_id = ds.store_id
        inner join {p_edw_schema}.dim_store ds1 
            on ra.receive_org_id = ds1.store_id
        inner join {p_edw_schema}.dim_product_sku dpc
            on ra.product_id = dpc.product_id 
                and ra.color_id = dpc.color_id
                and ra.size_id = dpc.size_id
        where ds1.store_type = '加盟' and tsj.c_store_id is null 
    '''
    
    spark.create_temp_table(sql_target_peacebird, 'target_peacebird')
    spark.create_temp_table(sql_store_trans_status, 'store_trans_status')
    spark.create_temp_table(sql_store_skc_trans_status, 'store_skc_trans_status')
    spark.create_temp_table(sql_replenish_notin, 'replenish_notin')
    spark.create_temp_table(sql_replenish, 'replenish')
    spark.create_temp_table(sql_tmp_store_jm, 'tmp_store_jm')
    
    
    spark.execute_sql(sql_insert_re_zy)
    spark.execute_sql(sql_insert_re_jm)
    
    spark.drop_temp_table('replenish')
    spark.drop_temp_table('target_peacebird')
    spark.drop_temp_table('replenish_notin')
