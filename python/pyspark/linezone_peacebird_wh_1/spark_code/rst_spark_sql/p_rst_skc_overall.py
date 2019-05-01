# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_skc_overall
# Author: zsm
# Date: 2018/9/12 17:03
# ----------------------------------------------------------------------------
import os
from utils.tools import SparkInit


if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)

    sql_tmp1 = '''
        select
                a.product_id
                , a.color_id
                --两周预测销量上下限
                , concat( coalesce(cast(a.two_week_sales_floor_qty as int), '-'), '-', coalesce(cast(a.two_week_sales_celling_qty as int), '-') ) as two_week_sales_qty_pre
                --四周预测销量上下限  
                , concat( coalesce(cast(a.four_week_sales_floor_qty as int), '-'), '-', coalesce(cast(a.four_week_sales_celling_qty as int), '-') ) as four_week_sales_qty_pre
                --剩余生命周期预测销量上下限  
                , concat( coalesce(cast(a.residue_sales_floor_qty as int), '-'), '-', coalesce(cast(a.residue_sales_celling_qty as int), '-') ) as residue_sales_pre
                --未来四周预测销量
                , a.four_week_sales_qty
            from {p_edw_schema}.mod_skc_day_sales_prediction a
            where a.pre_day_date = '{p_input_date}'   --取今天的数据
                and org_type='男装'
    '''

    sql_tmp_store_count = '''
        select
            count(*) as store_count
        from {p_edw_schema}.dim_target_store
        where is_store = 'Y' and status='正常'   -- 477
    '''

    sql_tmp_skc_country_available_distributed = '''
        select
            a.product_code
            , a.color_code
            , count(1) as distributed_count
        --from dm.dm_skc_city_day_available_distributed
        from {p_dm_schema}.dm_skc_store_day_available_fullsize a
        inner join (select store_code from {p_edw_schema}.dim_target_store where is_store = 'Y' and status='正常') b 
            on a.store_code = b.store_code
        where day_date = '{p_input_date}'
        group by a.product_code,a.color_code
    '''

    sql_tmp_skc_country_available_distributed_rate = '''
        select
            product_code
            , color_code
            , distributed_count*1.0/store_count as distributed_rate
        from tmp_skc_country_available_distributed
        inner join tmp_store_count on 1=1
    '''

    sql_tmp_skc_country_add_order_info = '''
        select b.product_code
            , b.color_code
            , sum(a.add_order_qty) as add_order_qty
            , sum(a.arrived_qty) as arrived_qty
            --, sum(a.not_arrived_qty) as not_arrived_qty
            , (
                case
                    when  sum(a.not_arrived_qty) < 0 then 0
                    else sum(a.not_arrived_qty)
                end
                ) as not_arrived_qty
            , current_timestamp as etl_time
        from {p_edw_schema}.mid_sku_add_order_info a 
        inner join {p_edw_schema}.dim_product_sku b on a.sku_id = b.sku_id
        group by b.product_code, b.color_code
    '''

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_skc_overall partition(day_date)
        select
            b.year_id as year
            , b.quarter_id as quarter
            , b.tiny_class
            , a.product_code
            , dps.product_name
            , a.color_code
            , dps.color_name
            , a.pre_class
            , a.stage_lifetime
            , a.sales_out_rate  -- 售罄率
            , coalesce(g.brokensize_rate, '-') as brokensize_rate --断码率
            , coalesce(tdr.distributed_rate, '-') as distributed_rate  --铺货率
            , concat( a.last_seven_days_sales_qty, '/' ,a.total_sales_qty ) as sales
            , concat( coalesce(dscao.add_order_qty, '-'), '/' , coalesce(dscao.not_arrived_qty, '-') ) as add_order_info
            , a.warehouse_total_stock_qty as warehouse_total_stock_qty  -- 总仓库存
            , coalesce(e.two_week_sales_qty_pre, '---') as two_week_sales_qty_pre
            , coalesce(e.four_week_sales_qty_pre, '---') as four_week_sales_qty_pre
            , coalesce(e.residue_sales_pre, '---') as residue_sales_pre
            -- 建议追单量 = 四周预测销量 - 在途库存 - 总仓库存 - 门店库存 - 追单未到货量
            --当 四周预测销量 或 追单未到货量 为空时，则建议追单量为'-', 且追单未到货量为负时，则建立追单量为 0 
            , ( case 
                    when ( (e.four_week_sales_qty is null) or (dscao.not_arrived_qty is null) ) then '-'
                    when ( e.four_week_sales_qty*1.0-a.road_stock_qty-a.warehouse_total_stock_qty
                                -a.store_total_stock_qty-dscao.not_arrived_qty ) < 0  then 0
                    else ceil( e.four_week_sales_qty-a.road_stock_qty-a.warehouse_total_stock_qty
                                -a.store_total_stock_qty-dscao.not_arrived_qty )
                end
               ) as sug_add_order_qty  -- 建议追单量 
            --当 四周预测销量 或 不追单销售损失为空时，则不追单销售损失为'-'，且不追单销售损失为负时，则建立追单量为 0
            , ( case 
                    when ( (e.four_week_sales_qty is null) or (dscao.not_arrived_qty is null) ) then '-'
                    when ( e.four_week_sales_qty*1.0/2-a.road_stock_qty-a.warehouse_total_stock_qty
                                -a.store_total_stock_qty-dscao.not_arrived_qty ) < 0  then 0
                    else ceil( e.four_week_sales_qty*1.0/2-a.road_stock_qty-a.warehouse_total_stock_qty
                                -a.store_total_stock_qty-dscao.not_arrived_qty )
                end
               ) as not_add_order_sales_loss --不追单销售损失
            , current_timestamp as etl_time 
            , a.store_total_stock_qty
            , a.total_sales_qty
            , '{p_input_date}' as day_date
        from {p_dm_schema}.dm_skc_country_day_pre_classify a
        inner join {p_edw_schema}.dim_product b   -- 取skc所属的年份、季节、小类
            on a.product_code = b.product_code
        inner join {p_edw_schema}.dim_product_skc dps        		--取商品名称，颜色名称
            on a.product_code = dps.product_code 
            and a.color_code = dps.color_code
        left join {p_dm_schema}.dm_skc_country_day_brokensize_rate g  --取断码率
            on a.product_code = g.product_code 
            and a.color_code = g.color_code
            and a.day_date = g.day_date
        left join tmp_skc_country_add_order_info dscao  -- 取追单数量 add_order_qty 及追单未到货数量 not_arrived_qty
            on a.product_code = dscao.product_code 
            and a.color_code = dscao.color_code
        left join tmp1 e
            on e.product_id = dps.product_id 
            and e.color_id = dps.color_id
        left join tmp_skc_country_available_distributed_rate tdr  --取铺货率
            on tdr.product_code = a.product_code
            and tdr.color_code = a.color_code
        where a.day_date = '{p_input_date}'

    '''

    spark.create_temp_table(sql_tmp1, 'tmp1')
    spark.create_temp_table(sql_tmp_store_count, 'tmp_store_count')
    spark.create_temp_table(sql_tmp_skc_country_available_distributed, 'tmp_skc_country_available_distributed')
    spark.create_temp_table(sql_tmp_skc_country_available_distributed_rate, 'tmp_skc_country_available_distributed_rate')
    spark.create_temp_table(sql_tmp_skc_country_add_order_info, 'tmp_skc_country_add_order_info')
    spark.execute_sql(sql_insert)
    spark.drop_temp_table('tmp1')
    spark.drop_temp_table('tmp_store_count')
    spark.drop_temp_table('tmp_skc_country_available_distributed')
    spark.drop_temp_table('tmp_skc_country_available_distributed_rate')
    spark.drop_temp_table('tmp_skc_country_add_order_info')
