# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_skc_country_day_pre_classify
   Description :
   Author :       yangming
   date：          2018/9/10
-------------------------------------------------
   Change Activity:
                   2018/9/10:
-------------------------------------------------
"""
import os

from utils.tools import SparkInit

if __name__ == '__main__':
    # app_name
    file_name = os.path.basename(__file__)

    # 初始化spark环境
    foo = SparkInit(file_name)

    # 取出全国的目标产品
    sql_tmp_dim_target_product = """          
        select product_id
        from {p_edw_schema}.dim_target_product
        where day_date = '{p_input_date}'
        group by product_id
    """

    # 计算在途库存（总仓和门店）
    sql_tmp_day_road_stock = """
        select 
            a.product_code
            , a.color_code
            , '{p_input_date}' as day_date
            , sum(a.road_stock_qty) as road_stock_qty
            , current_timestamp as etl_time 
        from {p_edw_schema}.mid_sku_org_day_road_stock_peacebird a
        inner join (select * from {p_edw_schema}.dim_store where (is_store='Y' and store_type='自营' )or store_code='CB37') as ds
        on a.org_id = ds.store_id
        inner join tmp_dim_target_product tdtp   --筛选当季产品
        on a.product_id = tdtp.product_id
        where a.day_date='{p_input_date}'
        group by a.product_code, a.color_code
    """

    # skc整体情况表
    sql_tmp_skc_whole_situation = """
        select d.product_code
            , d.color_code
            , d.distributed_days
            , d.life_cycle_week as stage_lifetime
            , coalesce(a.last_seven_days_sales_qty,0) as last_seven_days_sales_qty
            , coalesce(a.total_sales_qty,0) as total_sales_qty
            , coalesce(c.warehouse_total_stock_qty, 0) as warehouse_total_stock_qty
            , coalesce(c.store_total_stock_qty, 0) as store_total_stock_qty
            , coalesce(b.road_stock_qty,0) as road_stock_qty
            , (coalesce(a.total_sales_qty,0) 
                + coalesce(b.road_stock_qty,0) 
                + coalesce(c.store_total_stock_qty, 0)
                + coalesce(c.warehouse_total_stock_qty, 0)
                ) as product_total
        from {p_dm_schema}.dm_skc_country_day_life_cycle d
        left join {p_dm_schema}.dm_skc_country_day_stock c
            on c.product_code = d.product_code and c.color_code = d.color_code
                and c.day_date = d.day_date
        left join tmp_day_road_stock b -- 取在途库存road_stock_qty（不仅包括门店的在途库存还包括其他的）
            on d.product_code = b.product_code and d.color_code = b.color_code
        left join {p_dm_schema}.dm_skc_country_day_sales a on a.product_code = d.product_code 
              and a.color_code = d.color_code and a.day_date = d.day_date
        where d.day_date = '{p_input_date}'
    """

    # 取年份季节最新的上下限band
    sql_tmp_mod_skc_expected_class_basis = """
        with tmp1 as (
        select 
            class_celling_slope
            , class_floor_slope
            , etl_time
            , quarter_id
            , year_id
            , row_number() over(partition by year_id, quarter_id order by etl_time desc) as new_index --按年份季节分组，更新时间倒序排列
        from {p_edw_schema}.mod_skc_expected_class_basis)
        select 
            class_celling_slope    
            , class_floor_slope
            , etl_time
            , quarter_id
            , year_id
        from tmp1
        where new_index = 1 --取最新记录
    """

    tmp_dim_target_product = foo.create_temp_table(sql_tmp_dim_target_product, 'tmp_dim_target_product')
    tmp_day_road_stock = foo.create_temp_table(sql_tmp_day_road_stock, 'tmp_day_road_stock')
    tmp_skc_whole_situation = foo.create_temp_table(sql_tmp_skc_whole_situation, 'tmp_skc_whole_situation')
    tmp_mod_skc_expected_class_basis = foo.create_temp_table(sql_tmp_mod_skc_expected_class_basis,
                                                             'tmp_mod_skc_expected_class_basis')

    # --假设 累计销量/(累计销量*时间间隔） = a
    # --  当 a>预期分类斜率上限时 				表示超出预期 用‘high’表示
    # --  当 预期分类斜率下限<a<预期分类斜率上限  表示预期内	 用‘conform’表示
    # --  当 a<预期分类斜率上限时   				表示低于预期 用‘low’表示
    sql = """
        insert overwrite table {p_dm_schema}.dm_skc_country_day_pre_classify
        partition(day_date)
        select a.product_code
            , a.color_code
            , a.distributed_days
            , a.stage_lifetime
            , a.last_seven_days_sales_qty
            , a.total_sales_qty
            , a.warehouse_total_stock_qty
            , a.store_total_stock_qty
            , a.road_stock_qty
            , (case when a.product_total <> 0 and a.total_sales_qty*1.0/a.product_total >= 1 then 1
                    when a.product_total <> 0 and a.total_sales_qty*1.0/a.product_total < 1 
                            and a.total_sales_qty*1.0/a.product_total > 0 then a.total_sales_qty*1.0/a.product_total
                    else 0 end) as sales_out_rate
            , (case when (a.product_total <> 0 
                        and (a.total_sales_qty*1.0/(a.product_total*a.distributed_days)) > c.class_celling_slope) then 'high'
                    when (a.product_total <> 0 
                        and (a.total_sales_qty*1.0/(a.product_total*a.distributed_days)) <= c.class_celling_slope
                        and (a.total_sales_qty*1.0/(a.product_total*a.distributed_days))>=c.class_floor_slope) then 'conform'
                    else 'low' end) as pre_class
            , current_timestamp as etl_time 
            , '{p_input_date}' as day_date
        from tmp_skc_whole_situation a
        inner join {p_edw_schema}.dim_product b   -- 取skc所属的年份、季节
            on a.product_code = b.product_code
        inner join tmp_mod_skc_expected_class_basis c  -- 取预期分类斜率的上限和下限
            on b.year_id = c.year_id and b.quarter_id = c.quarter_id
    """

    foo.execute_sql(sql)

    foo.drop_temp_table('tmp_dim_target_product')
    foo.drop_temp_table('tmp_day_road_stock')
    foo.drop_temp_table('tmp_skc_whole_situation')
    foo.drop_temp_table('tmp_mod_skc_expected_class_basis')
