# -*- coding: utf-8 -*-

# --------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_customer_store_alarm_bak_drp
# Author: liuhai
# Date: 2019/01/07 14:38
# Modified: 2019/01/07 14:38
# ----------------------------------------------------------------------------

from datetime import datetime, timedelta
import os
import sys
from utils.tools import SparkInit


p_date = sys.argv[1]

# 传入日期 加 1天
p_input_date_add_one = datetime.strptime(p_date, "%Y-%m-%d")+ timedelta(days=1)

# 获取星期几 星期一  1  星期二 2  int
weekday_id = p_input_date_add_one.isoweekday()
#print(weekday_id)

if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)   

    #取当天 能参与补货的 门店
    #sql_tmp_replenish_store = '''
    #    select 
    #        store_id
    #    from edw.mid_store_move_period
    #    where day_date = date_add('{p_input_date}', 1)
    #'''


    # 取传入日期当天的 参与st加盟商门店的相关信息
    sql_tmp_customer_store = '''
        SELECT 
            customer_id, store_id
            , discount, fund_pass, feecantake
            , day_date
        FROM edw.fct_customer_store_toc
        where day_date = '{p_input_date}'
    '''

    # -- 将 周一到周日 用 1…… 7 拼接起来
    sql_tmp_joinstore = '''
        select 
            c_store_id, 
            concat(monday , tuesday
                          , wednesday
                          , thursday
                          , friday
                          , saturday
                          , sunday) as con_1    
        from peacebird.st_joinstore_view
    '''

    # 取参与了补货但 没有达到 资格线的加盟商门店
    sql_tmp_customer_store_n = '''
        select
            b.customer_id as c_customer_id
            , a.c_store_id
            , b.feecantake as amount_can
            , b.fund_pass as amount_pass
        from tmp_joinstore a
        left join tmp_customer_store b 
            on a.c_store_id = b.store_id
        where substr(a.con_1, {weekday_id}, 1) = 'Y'   -- 只取 补货的门店  'Y'表示 补货 加盟
            and feecantake < fund_pass
    '''.format(**{"weekday_id": weekday_id})

    # 取 参与 st补货的加盟下门店按模型给出的结果补货所需的金额
    sql_tmp_customer_store_replenish_amount_all = '''
        SELECT 
            cs.customer_id as c_customer_id
            , cs.store_id as c_store_id
            , min(cs.feecantake) as amount_can
            , sum(mr.send_qty*cs.discount*dp.tag_price) as amount_bill
            , min(cs.fund_pass) as amount_pass
        FROM tmp_customer_store cs
        inner join edw.mod_sku_day_replenish mr
            on cs.store_id = mr.receive_store_id
                and date_add(cs.day_date, 1) = mr.dec_day_date   
        inner join edw.dim_product dp 
            on mr.product_id = dp.product_id
        group by cs.customer_id, cs.store_id
    '''

    # 在求正累和的时候，如果有 null 会对结果造成影响
    # 单独处理 有补货的 加盟商门店 按 每个加盟商下面 的门店所需的提货金额正序排列求累和

    sql_tmp_replenish_store_y = '''
        SELECT 
            c_customer_id
            , c_store_id
            , amount_can
            , amount_bill
            , amount_pass
            , sum(amount_bill) over(partition by c_customer_id order by amount_bill) as add_amount  -- 正累和
        FROM tmp_customer_store_replenish_amount_all
    '''

    sql_tmp_replenish_store_y1 = '''
        SELECT 
            c_customer_id
            , c_store_id
            , amount_can
            , amount_bill
            , amount_pass
            , ( case
                    when amount_can >= add_amount then 'Y'
                    else 'N'
                end
                ) as is_bill
        FROM tmp_replenish_store_y
    '''

    # 取门店全部可以补货的加盟商
    # 
    sql_tmp_customer_all = '''
        with tmp1 as (   -- 1.排除部分门店可以补货的加盟商
            select 
                c_customer_id
                , is_bill
                , count(1) over(partition by c_customer_id) as sum_1
            from tmp_replenish_store_y1
            group by c_customer_id, is_bill
        )
        SELECT 
            c_customer_id
        FROM tmp1
        where is_bill = 'N' or sum_1 > 1  -- 排除 门店全部可以补货的加盟商
        group by c_customer_id
    '''    

    # 组合成所需的表

    sql_insert = '''
        insert overwrite table {p_rst_schema}.rst_customer_store_alarm_bak_drp partition(billdate)
        -- 没有参与补货的加盟商的门店
        select 
            c_customer_id
            , c_store_id
            , amount_can
            , 0 as amount_bill   --没有达到资格线的，为0
            , amount_pass
            , 'N' as is_bill   -- 没有没有参与补货的门店 下单情况置为 'N'
            , current_timestamp as etl_time
            , from_unixtime(unix_timestamp('{p_input_date_add_one_day}', 'yyyy-MM-dd'), 'yyyyMMdd') as billdate 
        from tmp_customer_store_n

        union all 

        -- 当 可提货金额大于等于累和金额的门店 下单 情况为 Y
        select 
            a.c_customer_id
            , a.c_store_id
            , a.amount_can
            , a.amount_bill
            , a.amount_pass
            , a.is_bill 
            , current_timestamp as etl_time
            , from_unixtime(unix_timestamp('{p_input_date_add_one_day}', 'yyyy-MM-dd'), 'yyyyMMdd') as billdate 
        from tmp_replenish_store_y1 a 
        inner join tmp_customer_all b
            on a.c_customer_id = b.c_customer_id    
    '''

    spark.create_temp_table(sql_tmp_customer_store, 'tmp_customer_store')
    spark.create_temp_table(sql_tmp_joinstore, 'tmp_joinstore')
    spark.create_temp_table(sql_tmp_customer_store_n, 'tmp_customer_store_n')
    spark.create_temp_table(sql_tmp_customer_store_replenish_amount_all, 'tmp_customer_store_replenish_amount_all')
    spark.create_temp_table(sql_tmp_replenish_store_y, 'tmp_replenish_store_y')
    spark.create_temp_table(sql_tmp_replenish_store_y1, 'tmp_replenish_store_y1')
    spark.create_temp_table(sql_tmp_customer_all, 'tmp_customer_all')

        
    
    spark.execute_sql(sql_insert)
    
    
    spark.drop_temp_table('tmp_customer_store')
    spark.drop_temp_table('tmp_joinstore')
    spark.drop_temp_table('tmp_customer_store_n')
    spark.drop_temp_table('tmp_customer_store_replenish_amount_all')
    spark.drop_temp_table('tmp_replenish_store_y')
    spark.drop_temp_table('tmp_replenish_store_y1')
    spark.drop_temp_table('tmp_customer_all')
    
