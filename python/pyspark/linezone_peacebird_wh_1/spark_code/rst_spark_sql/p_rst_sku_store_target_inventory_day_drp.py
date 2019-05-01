# -*- coding: utf-8 -*-

# --------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_sku_store_target_inventory_day_drp
# Author: liuhai
# Date: 2018/11/05 14:38
# Modified: 2018/11/15 14:08 增加 门店的筛选
# Modified: 2018/11/23 23:08 修改 目标库存的计算逻辑
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit

if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)
    
    # 模型目标库存去重
    sql_quchong = '''
        select 
            product_id, color_id, size_id
            , store_id
            , max(target_stock) as target_stock   --, max(it_vd) as it_vd
        from edw.mod_sku_store_day_target_inventory
        where day_date = date_add('{p_input_date}',1)
        group by product_id, color_id, size_id, store_id
    '''
    
    # 取满足条件的门店
    sql_tmp_store = '''
            select
                store_id
                , store_type
                , is_gap
            from edw.dim_store ds
            --where ds.store_type = '自营' and ds.status = '正常' and ds.is_store = 'Y' and ds.is_toc = 'Y'
            where ds.is_store = 'Y' and ds.is_toc = 'Y' -- 2018年12月19日16:38:43 在生成笛卡尔积时 取 is_toc = 'Y' 的门店，不管是不是正常的  
    '''

    #  -- 取传入日期当天去重后 的 目标产品id
    sql_tmp_target_product = '''
        select 
            product_id 
        from edw.dim_target_product 
        where day_date = '{p_input_date}'
        group by product_id --, year_id, quarter_id
    '''
    
    # 取 目标产品 的 sku
    sql_tmp_product = '''
        select 
            a.product_id, a.color_id, a.size_id, a.sku_id, a.sku_code
        from edw.dim_product_sku_mod a
        inner join tmp_target_product b 
            on a.product_id = b.product_id 
    '''
    
    # 产品 和 门店 做 笛卡尔积
    sql_tmp_product_store_car_1 = '''
        SELECT 
            ts.store_id as c_store_id               -- 门店id
            , tp.product_id as m_product_id         -- 款号id
            , tp.sku_id as skuid                    -- 条码id
            , null as target                        -- 目标库存
            , null as onhand         -- 在店库存（当前库存）  为空补零
            , null as onway     -- 在途库存  -- 为空补零
            , 'N' as active
            , '1' as kind                           -- 性质 1 成衣 2 饰品 3 其他
            , null as ad_client_id
            , null as ad_org_id
            , 'Y' as isactive                       -- 是否可用 默认 'Y'
            , mav.m_attributesetinstance_id                -- 颜色尺寸
            , 'AI系统生成' as adjust_reason         -- 默认 'AI系统生成'
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from tmp_product tp 
        cross join tmp_store ts
        inner join peacebird.m_product_alias_view mav   -- 条码关联取颜色尺寸
            on tp.sku_code = mav.no   -- 条码code关联
    '''

    # 取传入日期当天的目标库存，并去重
    sql_target_peacebird = '''
        SELECT 
            org_id, product_id, color_id, size_id, stock_qty, active
        FROM edw.mid_day_target_stock_peacebird      
        where stock_date = '{p_input_date}'
        group by org_id, product_id, color_id, size_id, stock_qty, active
    '''

    # 产品 和 门店 做 笛卡尔积 筛选掉 目标库存中已有的 记录
    sql_tmp_product_store_car_2 = '''    
        select 
            c.* 
        from tmp_product_store_car_1 c 
        left join edw.dim_product_sku a 
            on c.skuid = a.sku_id
        left join target_peacebird b 
            on c.c_store_id = b.org_id
                and a.product_id = b.product_id and a.color_id = b.color_id and a.size_id = b.size_id
        where b.product_id is null
    '''

    
    # 以太平鸟目标库存为主表，筛选门店，并关联 模型输出 的目标库存表
    # 1、取太平鸟目标库存的激活状态
    # 2、只有当 太平鸟目标库存>0 且 模型输出结果有值 时，才取模型的目标库存 否则取 太平鸟目标库存 
    
    '''
        peacebird 目标库存 left join 模型目标库存 
        
        peacebird       ai         结果
        --------------------------------
          > 0           not null    ai
          > 0           null        peacebird
          = 0           not null    peacebird 
          = 0           null        peacebird
        --------------------------------------
        总结为：
        --------------------------------------
         > 0           not null    ai
          其他            其他     peacebird
    '''
    
    sql_tmp_mod_target_inv = '''
        select 
            tp.product_id, tp.color_id
            , tp.org_id as store_id, tp.size_id
            , (case
                    when tp.stock_qty > 0 and msinv.target_stock is not null  then msinv.target_stock
                    else tp.stock_qty
                end) as target
            --, (case
            --        when msinv.target_stock is null then tp.active
            --        else 'Y'
            --    end) as active
            , tp.active
        from target_peacebird tp
        inner join tmp_store ds        -- 筛选出 正常且自营的门店
            on tp.org_id = ds.store_id 
        --left join edw.mod_sku_store_day_target_inventory msinv         -- 修改取数表 2018年11月22日15:36:51
        --left join edw.mod_sku_store_day_target_inventory msinv
        left join quchong msinv 
            on msinv.store_id = tp.org_id
                and msinv.product_id = tp.product_id
                and msinv.color_id = tp.color_id
                and msinv.size_id = tp.size_id
        --where msinv.day_date = date_add('{p_input_date}',1) 
    '''
    
    # 关联 sku表取sku_id 
    # 关联实时库存表取 qty当前库存  qtyperin:在途库存
    # 通过条码关联 m_product_alias_view 取 颜色尺寸
    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_sku_store_target_inventory_day_drp partition(day_date)
        --insert overwrite table {p_rst_schema}.rst_sku_store_target_inventory_day_drp_1123 partition(day_date)
        SELECT 
            mssv.store_id as c_store_id             -- 门第id
            , mssv.product_id as m_product_id       -- 款号id
            , dps.sku_id as skuid                   -- 条码id
            , cast(mssv.target as int) as target       -- 目标库存
            , coalesce(cast(fsv.qty as int),0) as onhand         -- 在店库存（当前库存）  为空补零
            , coalesce(cast(fsv.qtyprein as int),0) as onway     -- 在途库存  -- 为空补零
            --, 'Y' as active                         -- 以目标库存为主表      默认 Y
            , mssv.active
            , '1' as kind                           -- 性质 1 成衣 2 饰品 3 其他
            , null as ad_client_id
            , null as ad_org_id
            , 'Y' as isactive                       -- 是否可用 默认 'Y'
            , mav.m_attributesetinstance_id                -- 颜色尺寸
            , 'AI系统生成' as adjust_reason         -- 默认 'AI系统生成'
            , current_timestamp as etl_time
            , '{p_input_date}' as day_date
        from tmp_mod_target_inv mssv 
        inner join edw.dim_product_sku dps   -- 关联sku表取sku_id
            on mssv.product_id = dps.product_id 
                and mssv.color_id = dps.color_id
                and mssv.size_id = dps.size_id
        left join peacebird.fa_storage_view fsv    -- 关联实时库存表 取 qty当前库存  qtyperin:在途库存  
            on mssv.store_id = fsv.c_store_id
                and dps.sku_id = fsv.m_productalias_id
        inner join peacebird.m_product_alias_view mav   -- 条码关联取颜色尺寸
            on dps.sku_code = mav.no   -- 条码code关联
        inner join edw.dim_store ds
            on mssv.store_id = ds.store_id 

        union all    -- 将 筛选后 的 产品 和 门店 的 笛卡尔积 并入

        select 
            c.*
        from tmp_product_store_car_2 c 
    '''
    
    spark.create_temp_table(sql_quchong, 'quchong')
    spark.create_temp_table(sql_tmp_store, 'tmp_store')
    spark.create_temp_table(sql_tmp_target_product, 'tmp_target_product')
    spark.create_temp_table(sql_tmp_product, 'tmp_product')
    spark.create_temp_table(sql_tmp_product_store_car_1, 'tmp_product_store_car_1')
    spark.create_temp_table(sql_target_peacebird, 'target_peacebird')
    spark.create_temp_table(sql_tmp_product_store_car_2, 'tmp_product_store_car_2')
    spark.create_temp_table(sql_tmp_mod_target_inv, 'tmp_mod_target_inv')       
    
    spark.execute_sql(sql_insert)
    
    spark.drop_temp_table('target_peacebird')
    spark.drop_temp_table('tmp_product_store_car_2')
    spark.drop_temp_table('tmp_mod_target_inv')
    spark.drop_temp_table('tmp_product_store_car_1')
    spark.drop_temp_table('tmp_target_product')
    spark.drop_temp_table('tmp_product')
    spark.drop_temp_table('tmp_store')
    spark.drop_temp_table('quchong')
    
