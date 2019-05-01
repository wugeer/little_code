# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_day_end_stock_interval
   Description :
   Author :       Garrett Wang
   date：          2018/8/11
-------------------------------------------------
   Change Activity:
                   2018/8/11:
-------------------------------------------------
--
-- 项目: peacebird
-- 过程名: p_mid_day_end_stock
-- 源表:  edw.fct_month_end_stock
--        edw.fct_io
--        edw.dim_store
-- 目标表: edw.mid_day_end_stock
-- 程序描述: 日末库存表
-- 程序路径: /opt/peacebird/edw/p_mid_day_end_stock.sql
-- 程序备注:  
-- 版本号信息: v1.0 create
--             v1.1 修改为关联门店仓库表
--             v2.0 alter   改写pyspark
"""

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
from dateutil.parser import parse

import os
import sys
import datetime
import time
import random
import calendar
from datetime import timedelta
from concurrent import futures
from multiprocessing import cpu_count


class TaskProcessPoolExecutors(object):
    """
        使用concurrent.futures包多进程去异步执行任务
    """

    def __init__(self, task_list, handler):

        # 任务列表
        self._task_list = task_list
        # 处理逻辑
        self._handler = handler

        # 如果当前任务数大于cpu核数，则取CPU核数；否则使用当前任务数作为最大并行任务数；
        if len(task_list) > cpu_count():
            max_task_num = cpu_count()
        else:
            max_task_num = len(task_list)

        self._executor = futures.ProcessPoolExecutor(max_task_num)
        self._process()

    def _process(self):
        # 任务结果
        self._result = self._executor.map(self._handler, self._task_list)

    @property
    def result(self):
        # 将结果处理成一个数组返回
        return [elem for elem in self._result]


class TaskThreadPoolExecutors(object):
    """
        使用concurrent.futures包多线程去异步执行任务
    """

    def __init__(self, task_list, handler, max_task_num):

        # 任务列表，通常来说是参数列表
        self._task_list = task_list
        # 任务的核心处理逻辑
        self._handler = handler
        # 最大并行的任务数量
        self._max_task_num = max_task_num
        self._executor = futures.ThreadPoolExecutor(max_task_num)
        self._process()

    def _process(self):
        # 任务结果
        self._result = self._executor.map(self._handler, self._task_list)

    @property
    def result(self):
        # 将结果处理成一个数组返回
        return [elem for elem in self._result]


class MidDayEndStocks:

    def __init__(self):

        # 初始化spark环境
        warehouse_location = abspath('hdfs://master1:9000/user/hive/warehouse')
        # app_name
        file_name = os.path.basename(__file__)
        app_name = "".join(["PySpark-", file_name, str(random.random())])

        spark_conf = SparkConf()
        # 配置可用来做缓存cache
        #spark_conf.set("spark.storage.memoryFraction", 0.6)
        spark_conf.set("spark.sql.warehouse.dir", warehouse_location)
        # 合并map输出端的文件
        spark_conf.set("spark.shuffle.consolidateFiles", "true")
        spark_conf.set("spark.executor.memory", "2g")
        spark_conf.set("spark.executor.cores", "2")
        # 理想情况下,task数量设置成Spark Application 的总CPU核数,但是现实中总有一些task运行慢一些task快,导致快的先执行完,空余的cpu 核就浪费掉了,所以官方推荐task数量要设置成Spark Application的总cpu核数的2~3 倍
        spark_conf.set("spark.default.parallelism", "150")
        spark_conf.set("spark.driver.memory", "10g")
        # 构造sparksession对象
        self.spark_session = SparkSession.builder.config(conf = spark_conf).master('yarn').appName(app_name) \
            .enableHiveSupport().getOrCreate()


    @staticmethod
    def get_last2_month_end_day(v_p_input_date):
        """
        获取上上个月的月末日期
        :param v_p_input_date: 计算哪天的日末库存
        :return: 
        """
        # 获取上月天数
        # 对于上月是上年最后一天的情况
        if v_p_input_date.month - 1 == 0:
            monthRange = calendar.monthrange(v_p_input_date.year - 1, 12)
        else:
            monthRange = calendar.monthrange(v_p_input_date.year, v_p_input_date.month - 1)
        # 本月开始日期
        current_month_start = datetime.date(v_p_input_date.year, v_p_input_date.month, 1)
        # 本月开始日期-（上月天数+1）=上上个月月末日期
        last_two_month_end = current_month_start - timedelta(days = monthRange[1] + 1)
        v_stock_begin_day = last_two_month_end.strftime("%Y-%m-%d")

        return v_stock_begin_day

    def cal_sql_params(self, v_p_input_date):
        """
        构造计算日末库存的参数
        :param v_p_input_date: 计算哪天的日末库存
        :return: 构造的参数
        """
        v_p_input_date_fmt = parse(v_p_input_date.strftime("%Y-%m-%d"))
        # 取哪天的月结库存
        v_stock_begin_day = self.get_last2_month_end_day(v_p_input_date_fmt)
        res_dic = {"p_input_date": repr(v_p_input_date_fmt.strftime("%Y-%m-%d")),
                   "v_stock_begin_day": repr(v_stock_begin_day)}

        return res_dic

    @staticmethod
    def execute_mid_day_stock(spark_session, **kwargs):
        # --  计算月结库存, 并且计算净入库
        sql_tmp_mid_day_stock = """
                select {p_input_date} as stock_date
                    , fmes.org_id
                    , ds.store_type as org_type
                    , fmes.product_id
                    , fmes.color_id
                    , fmes.size_id
                    , fmes.stock_qty
                    , 0 as receive_qty
                    , 0 as send_qty
                from fct_month_end_stock_cached fmes
                inner join dim_store_cached ds on fmes.org_id=ds.store_id
                where fmes.stock_date = {v_stock_begin_day}
                union all 
                select {p_input_date} as stock_date
                , fi.org_id
                , max(ds.store_type) as org_type
                , fi.product_id
                , fi.color_id
                , fi.size_id
                , sum(fi.qty) as stock_qty
                , 0 as receive_qty
                , 0 as send_qty
                from fct_io_cached fi 
                inner join dim_store_cached ds on fi.org_id=ds.store_id
                where fi.io_date > {v_stock_begin_day} and fi.io_date <= {p_input_date}
                group by fi.org_id, fi.product_id, fi.color_id, fi.size_id
        """.format(**kwargs)

        tmp_mid_day_stock_data = spark_session.sql(sql_tmp_mid_day_stock)
        print('********** tmp_mid_day_stock_data.type = {0}'.format(type(tmp_mid_day_stock_data)))
        return tmp_mid_day_stock_data

    def cal_mid_day_end_stock(self, v_p_input_date):
        sql_params = self.cal_sql_params(v_p_input_date)
        mid_day_end_stock = self.execute_mid_day_stock(self.spark_session, **sql_params)
        return mid_day_end_stock

    def __enter__(self):
        self.start_time = time.time()
        print('********************************      program start - {0}    *******************************************'
              .format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.start_time)))
              )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        self.time_use = self.end_time - self.start_time
        print('********************************   program end -{0} all-{1}  *******************************************'
              .format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.end_time)), self.time_use)
              )
        return self


if __name__ == '__main__':

    # spark2-submit p_mid_day_end_stock.py 2018-06-01 2018-06-10
    # 解析传入的参数
    p_begin_date, p_end_date = parse(sys.argv[1]), parse(sys.argv[2])
    v_input_date = p_begin_date

    midDayEndStocks = MidDayEndStocks()
    # 缓存三张表：edw.dim_store
    store_df = midDayEndStocks.spark_session.sql("select * from edw.dim_store")
    store_df.registerTempTable("dim_store_cached")
    #midDayEndStocks.spark_session.cacheTable("dim_store_cached")
    #print("************* edw.dim_store_cached.count()" + store_df.count())

    # 缓存三张表：edw.fct_month_end_stock
    fct_month_end_stock_df = midDayEndStocks.spark_session.sql(
        """SELECT stock_date
            , org_id
            , product_id
            , color_id
            , size_id
            , stock_qty
        FROM edw.fct_month_end_stock
        WHERE stock_date <= '2018-12-31' AND stock_date >= '2016-10-31' """)
    fct_month_end_stock_df.registerTempTable("fct_month_end_stock_cached")
    #midDayEndStocks.spark_session.cacheTable("fct_month_end_stock_cached")
    #print("************* edw.fct_month_end_stock_cached.count()" + fct_month_end_stock_df.count())

    # 缓存三张表
    fct_io_df = midDayEndStocks.spark_session.sql(
        """select org_id
        , product_id
        , color_id
        , size_id
        , qty
        , io_date
        from edw.fct_io
        WHERE io_date >= '2016-10-31' AND io_date <= '2018-12-31' """)
    fct_io_df.registerTempTable("fct_io_cached")
    #midDayEndStocks.spark_session.cacheTable("fct_io_cached")
    #print("************* edw.fct_io_cached.count()" + fct_io_df.count())

    # 最终所有日末库存数据存储位置
    # 并行计算每一天日末库存
    day_end_list = []
    day_1 = timedelta(days=1)
    while v_input_date <= p_end_date:
        day_end_list.append(v_input_date)
        v_input_date += day_1
    mid_day_end_stocks = TaskThreadPoolExecutors(day_end_list, midDayEndStocks.cal_mid_day_end_stock, 20)

    # 获取多线程返回的结果
    mid_day_end_stock_datas = None
    for mid_day_end_stock in mid_day_end_stocks.result:
        if mid_day_end_stock_datas is None:
            mid_day_end_stock_datas = mid_day_end_stock
        else:
            mid_day_end_stock_datas = mid_day_end_stock_datas.unionAll(mid_day_end_stock)

    # 注册一个临时表
    mid_day_end_stock_datas.registerTempTable("mid_day_end_stocks")

    # 计算每天的库存
    sql_day_end_stock_into = """
        insert into table edw.mid_day_end_stock
        select
            stock_date
            , org_id
            , max(org_type) as org_type
            , product_id
            , color_id
            , size_id
            , sum(stock_qty) as stock_qty
            , sum(receive_qty) as receive_qty
            , sum(send_qty) as send_qty
            , current_timestamp as etl_time
        from mid_day_end_stocks
        group by stock_date, org_id, product_id, color_id, size_id
        """

    # 执行入库操作
    midDayEndStocks.spark_session.sql(sql_day_end_stock_into)
    midDayEndStocks.spark_session.catalog.dropTempView("mid_day_end_stocks")

    # 缓存三张表：edw.fct_month_end_stock，edw.dim_store，edw.fct_io
    #midDayEndStocks.spark_session.uncacheTable("dim_store_cached")
    #midDayEndStocks.spark_session.uncacheTable("fct_io_cached")
    #midDayEndStocks.spark_session.uncacheTable("fct_month_end_stock_cached")
    midDayEndStocks.spark_session.catalog.dropTempView("dim_store_cached")
    midDayEndStocks.spark_session.catalog.dropTempView("fct_io_cached")
    midDayEndStocks.spark_session.catalog.dropTempView("fct_month_end_stock_cached")
