# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_dm_skc_store_day_fullsize.py
   Description :
   Author :       zsm
   date：          2018/8/27
-------------------------------------------------
   Change Activity:
                   2018/8/27:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_dm_skc_store_day_fullsize.py
-- 源表: 
-- 目标表:dm.dm_skc_store_day_fullsize
-- 程序描述:
-- 程序路径: /opt/peacebird/zsm/p_dm_skc_store_day_fullsize.py
-- 程序备注:  
-- 版本号信息: v1.0 create
--             v2.0 alter   改写pyspark
"""

import sys
import time

import os
from os.path import abspath
from dateutil.parser import parse
from pyspark.sql import Window
import pyspark.sql.functions as func
from utils.tools import SparkInit
from utils.config import PBConfig


class IsFullsize(SparkInit):
    def __init__(self, file_name, mid_table, mid_col, target_table, target_col):
        # 继承SparkInit的__init__方法
        SparkInit.__init__(self, file_name)
        # 日期
        self.p_input_date = repr(parse(sys.argv[1]).strftime('%Y-%m-%d'))
        # 齐码规则
        self.small_fullsize_1 = repr('S,M,L')
        self.small_fullsize_2 = repr('M,L,XL')
        self.big_fullsize_1 = repr('M,L,XL')
        self.big_fullsize_2 = repr('L,XL,XXL')
        # 一个skc至少含多少个sku才判断是否齐码
        self.gap = 3
        # schema
        p_edw_schema = PBConfig.EDW_SCHEMA
        p_dm_schema = PBConfig.DM_SCHEMA
        p_rst_schema = PBConfig.RST_SCHEMA
        # 参数字典
        self.params_dict = {
            "p_input_date": self.p_input_date,
            "small_serial_fullsize_1": self.small_fullsize_1,
            "small_serial_fullsize_2": self.small_fullsize_2,
            "big_serial_fullsize_1": self.big_fullsize_1,
            "big_serial_fullsize_2": self.big_fullsize_2,
            "gap": self.gap,
            "mid_table": mid_table,
            "stock_qty": mid_col,
            "target_table": target_table,
            "is_fullsize": target_col,
            "p_edw_schema": p_edw_schema,
            "p_dm_schema": p_dm_schema,
            "p_rst_schema": p_rst_schema
        }
        self.fullsize_rule()

    def get_params(self):
        return None

    def fullsize_rule(self):
        # 1.每个大区的齐码规则
        sql = """
            select 
                org_id as dq_id
                , org_name as dq_name
                , (
                  case when 
                    org_name = '东北' or org_name = '华北' 
                        then {big_serial_fullsize_1} else {small_serial_fullsize_1} 
                    end
                  ) as serial_fullsize_1
                , (
                  case when 
                    org_name = '东北' or org_name = '华北' 
                        then {big_serial_fullsize_2} else {small_serial_fullsize_2} 
                    end
                  ) as serial_fullsize_2
            from {p_edw_schema}.dim_stockorg 
            where org_type = '大区'
            """
        # self.spark.sql(sql).createOrReplaceTempView("serial_fullsize")
        self.create_temp_table(sql, "serial_fullsize")
        # 2.准备输入数据
        self.input_data()

    def input_data(self):
        # 准备输入数据
        sql = """
            select 
                store.dq_id
                , org.org_name as dq_name
                , stock.store_code
                , stock.product_code
                , stock.color_code
                , stock.size_code
                , sku.size_name
                , serial_fullsize.serial_fullsize_1
                , serial_fullsize.serial_fullsize_2
            from {mid_table} as stock
            inner join {p_edw_schema}.dim_product_sku as sku
                on stock.product_code = sku.product_code
                and stock.color_code = sku.color_code
                and stock.size_code = sku.size_code
            inner join (select product_code,color_code from {p_edw_schema}.dim_product_sku group by product_code,color_code having count(1) >= {gap}) as skc
                on stock.product_code = skc.product_code
                and stock.color_code = skc.color_code
            inner join {p_edw_schema}.dim_store as store
                on stock.store_code = store.store_code
            inner join {p_edw_schema}.dim_stockorg as org
                on store.dq_id = org.org_id
            inner join serial_fullsize 
                on org.org_name = serial_fullsize.dq_name
            where 
                stock.{stock_qty}>0
                and stock.day_date = {p_input_date}
            """
        # self.spark.sql(sql).createOrReplaceTempView("input_data")
        self.create_temp_table(sql, "input_data")
        # input_data = self.spark.table("input_data")
        # self.is_fullsize(input_data)

        self.is_fullsize()

    # def is_fullsize(self, input_data):
    def is_fullsize(self):
        # 3.计算日末库存中大于零的skc在每个门店的连续尺码组合,将某门店有库存的sku的排列组合放在serial_size
        """
        serial_size = input_data\
                       .withColumn("serial_size_array",func.collect_set("input_data.size_name"))\
                        .over(Window\
                         .partitionBy("input_data.store_code","input_data.product_code","input_data.color_code")\
                         .orderBy("input_data.size_code")\
                         .rowsBetween(0,self._gap-1))\
                       .withColumn("serial_size",func.concat_ws(",","input_data.serial_size_array"))

        serial_size.createOrReplaceTempView("serial_fullsize")
        """
        sql = """
        select product_code,color_code,store_code,serial_fullsize_1,serial_fullsize_2
            ,concat_ws(',',collect_list(size_name) over(partition by store_code,product_code,color_code order by size_code 
                rows between current row and ({gap}-1) following)) as serial_size
        from input_data
        """
        # self.spark.sql(sql).createOrReplaceTempView("serial_fullsize")
        self.create_temp_table(sql, "serial_fullsize")
        self.insert_overwrite()

    def insert_overwrite(self):
        # 5.判断是否断码
        # 5.1.将每一个skc得每一种尺码组合与给定得规则做对比，匹配上记为1，匹配不上记为0
        sql = """
            select
                product_code
                , color_code
                , store_code
                , (case when serial_size = serial_fullsize_1 or serial_size = serial_fullsize_2 then 1 else 0 end) as counter
            from serial_fullsize
            """
        # self.spark.sql(sql).createOrReplaceTempView("is_fullsize")
        self.create_temp_table(sql, "is_fullsize")
        # 5.2.将每一个skc在每家门店是否断码插入hive表
        sql = """
            insert overwrite table {target_table} partition(day_date)
            select 
                product_code
                , color_code
                , store_code
                , (case when sum(counter) > 0 then 'Y' else 'N' end) as {is_fullsize}
                , current_timestamp as etl_time
                , {p_input_date} as day_date
            from is_fullsize
            group by 
                product_code
                ,color_code
                ,store_code
            """
        self.execute_sql(sql)
        self.drop_temp_table("serial_fullsize")
        self.drop_temp_table("input_data")
        self.drop_temp_table("is_fullsize")


if __name__ == '__main__':
    start_time = time.time()

    # 命令格式例：
    # spark2-submit p_dm_skc_store_day_fullsize.py 2018-08-01 {p_dm_schema}.dm_skc_store_day_available_fullsize available_is_fullsize \
    # {p_dm_schema}.dm_sku_store_day_available_stock available_stock_qty
    # spark2-submit p_dm_skc_store_day_fullsize.py 2018-08-01 dm.dm_skc_store_day_fullsize is_fullsize \
    # dm.dm_sku_store_day_stock stock_qty
    # spark2-submit p_dm_skc_store_day_fullsize.py 2018-08-01 {p_dm_schema}.dm_skc_store_day_after_replenish_allot_fullsize is_fullsize \
    # {p_dm_schema}.dm_sku_store_day_replenish_allot_stock after_replenish_allot_qty

    # 解析传入的参数
    target_table = sys.argv[2]
    target_col = sys.argv[3]
    mid_table = sys.argv[4]
    mid_col = sys.argv[5]

    file_name = os.path.basename(__file__)
    # 跑数据
    isFullsize = IsFullsize(file_name, mid_table, mid_col, target_table, target_col)

    print(time.time() - start_time)
