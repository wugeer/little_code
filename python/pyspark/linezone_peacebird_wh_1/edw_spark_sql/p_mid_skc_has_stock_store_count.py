# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     p_mid_skc_has_stock_store_count
   Description :
   Author :       yangming
   date：          2018/11/4
-------------------------------------------------
   Change Activity:
                   2018/11/4:
                   2018/12/26: 排除加盟门店对 有库存门店的影响
-------------------------------------------------
"""

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
import sys
from concurrent import futures
import datetime
from dateutil.parser import parse
from multiprocessing import cpu_count


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


class ThisTask:
    def __init__(self):
        warehouse_location = abspath('hdfs://master1:9000/user/hive/warehouse')

        # app_name
        file_name = os.path.basename(__file__)
        app_name = "".join(["PySpark-", file_name])

        # config 配置
        spark_conf = SparkConf()
        spark_conf.set("spark.sql.warehouse.dir", warehouse_location)
        # 解决 分区下 小文件过多的问题
        spark_conf.set("spark.sql.shuffle.partitions", '1')

        self.spark = SparkSession \
            .builder \
            .appName(app_name) \
            .config(conf=spark_conf) \
            .enableHiveSupport() \
            .getOrCreate()

    def exec_sql(self, p_input_date):
        params_dict = {'p_input_date': p_input_date}
        sql_tmp1 = """
            select a.product_id
                , a.color_id
                , a.org_id as store_id
                , sum(a.stock_qty) as stock_qty         -- 在店库存
            from edw.mid_day_end_stock a 
            inner join edw.dim_target_store b                  -- 排除加盟门店的影响
                on a.org_id = b.store_id
            where a.stock_date = '{p_input_date}' and b.is_store = 'Y'  
            group by a.product_id, a.color_id, a.org_id
            --union all 
            --select a.product_id
            --    , a.color_id
            --    , a.org_id as store_id
            --   , sum(a.road_stock_qty) as stock_qty    -- 在途库存
            --from edw.mid_sku_org_day_road_stock_peacebird a 
            --inner join edw.dim_target_store b                 -- 排除加盟门店的影响
            --    on a.org_id = b.store_id
            --where a.day_date = '{p_input_date}' and b.is_store = 'Y' 
            --group by a.product_id, a.color_id, a.org_id    
        """.format(**params_dict)

        sql_tmp2 = """
            select product_id, color_id, store_id, sum(stock_qty) as stock_qty
            from tmp1 
            group by product_id, color_id, store_id
        """

        sql = """
            insert overwrite table edw.mid_skc_has_stock_store_count
            partition(day_date)
            select a.product_id
                , max(b.product_code) as product_code
                , a.color_id
                , max(b.color_code) as color_code
                , count(1) as has_stock_store_count
                , current_timestamp as etl_time
                , '{p_input_date}' as day_date
            from tmp1 a 
            inner join edw.dim_product_skc b on a.product_id = b.product_id
                and a.color_id = b.color_id
            where a.stock_qty > 0
            group by a.product_id, a.color_id
        """.format(**params_dict)

        tmp1 = self.spark.sql(sql_tmp1).createOrReplaceTempView("tmp1")
        #tmp2 = self.spark.sql(sql_tmp2).createOrReplaceTempView("tmp2")

        print(sql)
        self.spark.sql(sql)

        # drop 临时表
        self.spark.catalog.dropTempView("tmp1")
        #self.spark.catalog.dropTempView("tmp2")


if __name__ == '__main__':
    argv_length = len(sys.argv)
    day_1 = datetime.timedelta(days=1)
    if argv_length == 1:
        # spark2-submit p_mid_skc_has_stock_store_count.py                               # 运行昨天数据
        p_begin_date = (datetime.date.today() - day_1).strftime('%Y-%m-%d')
        print(p_begin_date)
        foo = ThisTask()
        foo.exec_sql(p_begin_date)
    elif argv_length == 2:
        # spark2-submit p_mid_skc_has_stock_store_count.py 2018-11-03
        p_begin_date = parse(sys.argv[1]).strftime("%Y-%m-%d")
        foo = ThisTask()
        foo.exec_sql(p_begin_date)
    elif argv_length == 3:
        # spark2-submit p_mid_skc_has_stock_store_count.py 2018-09-01 2018-11-03
        p_begin_date, p_end_date = parse(sys.argv[1]), parse(sys.argv[2])
        v_input_date = p_begin_date
        foo = ThisTask()
        day_end_list = []
        while v_input_date <= p_end_date:
            day_end_list.append(v_input_date.strftime("%Y-%m-%d"))
            foo.exec_sql(v_input_date.strftime("%Y-%m-%d"))
            v_input_date += day_1
            
        # foos = TaskProcessPoolExecutors(day_end_list, foo.exec_sql)
        # res_list = [res for res in foos.result]

    else:
        print('params error!!!')

