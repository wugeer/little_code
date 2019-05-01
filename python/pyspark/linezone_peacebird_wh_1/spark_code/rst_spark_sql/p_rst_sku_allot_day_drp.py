# -*- coding: utf-8 -*-

# --------------------------------------------------------------------------
# Project: peacebird-wh-sql
# Filename: p_rst_sku_allot_day_drp
# Author: liuhai
# Date: 2018/11/03 09:30
# ----------------------------------------------------------------------------

import os
from utils.tools import SparkInit

from datetime import datetime

if __name__ == '__main__':
    file_name = os.path.basename(__file__)
    spark = SparkInit(file_name)
    
    #判断传入日期是否等于星期天
    if datetime.strptime(spark.params_dict['p_input_date'],'%Y-%m-%d').weekday() == 6:
        print('星期天')
        #取调拨调拨数据
        sql_allot = '''
            select           --sku
                product_id
                , color_id
                , size_id
                , send_store_id as send_org_id
                , receive_store_id as receive_org_id            
                , send_qty 
                -- eg: select from_unixtime(unix_timestamp('2018-10-11', 'yyyy-mm-dd'), 'yyyymmdd')
                , from_unixtime(unix_timestamp(dec_day_date, 'yyyy-mm-dd'), 'yyyymmdd') as billdate
            from {p_edw_schema}.mod_sku_day_allot
            where dec_day_date = date_add('{p_input_date}',1)
        '''
        
        # 将 send_org_code+receive_org_code+billdate 拼成所需的 refno 
        sql_insert = '''
            insert overwrite table {p_rst_schema}.rst_sku_allot_day_drp partition(billdate)
            select 
                ds.store_code as send_org_code
                , ds1.store_code as receive_org_code
                , concat(ds.store_code,'+',ds1.store_code,'+',ra.billdate) as refno
                , dpc.sku_code
                , ra.send_qty as qty            
                , current_timestamp as etl_time
                , ra.billdate
            from allot ra 
            inner join {p_edw_schema}.dim_store ds 
                on ra.send_org_id = ds.store_id
            inner join {p_edw_schema}.dim_store ds1 
                on ra.receive_org_id = ds1.store_id
            inner join {p_edw_schema}.dim_product_sku dpc
                on ra.product_id = dpc.product_id 
                    and ra.color_id = dpc.color_id
                    and ra.size_id = dpc.size_id
        '''

        spark.create_temp_table(sql_allot, 'allot')
        
        spark.execute_sql(sql_insert)
        
        spark.drop_temp_table('allot')
