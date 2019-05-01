# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     tools
   Description :    通用工具包
   Author :       yangming
   date：          2018/9/3
-------------------------------------------------
   Change Activity:
                   2018/9/3:
-------------------------------------------------
"""
import sys
import datetime
from datetime import timedelta

from dateutil.parser import parse
from multiprocessing import cpu_count
from concurrent import futures
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from utils.config import PBConfig


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

    def __init__(self, handler, max_task_num, *task_list):
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
        self._result = self._executor.map(self._handler, *self._task_list)

    @property
    def result(self):
        # 将结果处理成一个数组返回
        return [elem for elem in self._result]


class SparkInit(object):
    def __init__(self, file_name):
        warehouse_location = abspath('hdfs://master1:9000/user/hive/warehouse')
        # app的名称
        app_name = "".join(["PySpark-", file_name])

        # config 配置
        spark_conf = SparkConf()
        spark_conf.set("spark.sql.warehouse.dir", warehouse_location)
        spark_conf.set("hive.exec.dynamic.partition.mode", 'nonstrict')
        # 目的是解决报错：Detected cartesian product for INNER join between logical plans
        spark_conf.set("spark.sql.crossJoin.enabled", 'true')
        # 解决 分区下 小文件过多的问题
        spark_conf.set("spark.sql.shuffle.partitions", '1')

        self.spark = SparkSession \
            .builder \
            .appName(app_name) \
            .config(conf=spark_conf) \
            .enableHiveSupport() \
            .getOrCreate()
        # TODO udf需要再次调试
        self.register_udf()

        # 获取脚本执行需要的参数
        self.params_dict = self.get_params()

    def register_udf(self):
        """
        注册udf使用
        :return: 
        """
        # # 注册udf
        self.spark.udf.register('udf_date_trunc', date_trunc)

    @staticmethod
    def get_params():
        """
        获取参数, 返回python脚本的参数字典, 
        :return: params_dict 默认返回输入日期和输入日期截断到周一的日期, 正价率的字段，畅销款比率
        """
        param_list = sys.argv
        if len(param_list) != 3 and len(param_list) != 2 and len(param_list) != 1:
            raise Exception('参数错误')
        else:
            p_input_date = parse(param_list[1]).strftime('%Y-%m-%d')
            p_input_date_mon = date_trunc('week', p_input_date)
            p_input_date_add_one_day = (parse(p_input_date) + timedelta(days=1)).strftime('%Y-%m-%d')
            p_input_date_add_one_mon = date_trunc('week', (parse(p_input_date) + timedelta(days=1)).strftime('%Y-%m-%d'))
            p_input_date_sub_six_mon = date_trunc('week', (parse(p_input_date) - timedelta(days=6)).strftime('%Y-%m-%d'))
            p_input_date_year = date_trunc('year', p_input_date)
            p_full_price_rate = PBConfig.FULL_PRICE_RATE
            p_sell_well_rate = PBConfig.SELL_WELL_RATE
            p_edw_schema = PBConfig.EDW_SCHEMA
            p_dm_schema = PBConfig.DM_SCHEMA
            p_rst_schema = PBConfig.RST_SCHEMA

            params_dict = {'p_input_date': p_input_date,
                           'p_input_date_add_one_day': p_input_date_add_one_day,
                           'p_input_date_mon': p_input_date_mon,
                           'p_input_date_add_one_mon': p_input_date_add_one_mon,
                           'p_input_date_sub_six_mon': p_input_date_sub_six_mon,
                           'p_input_date_year': p_input_date_year,
                           'p_full_price_rate': p_full_price_rate,
                           'p_sell_well_rate': p_sell_well_rate,
                           'p_edw_schema': p_edw_schema,
                           'p_dm_schema': p_dm_schema,
                           'p_rst_schema': p_rst_schema}
            return params_dict

    def create_temp_table(self, sql, table_name):
        """
        创建临时表
        :param sql: sql语句
        :param table_name: 临时表表名
        :return: 
        """
        sql_temp = sql.format(**self.params_dict)
        temp_table = self.spark.sql(sql_temp).createOrReplaceTempView(table_name)
        return temp_table

    def drop_temp_table(self, table_name):
        """
        drop临时表
        :param table_name: 
        :return: 
        """
        self.spark.catalog.dropTempView(table_name)

    def execute_sql(self, sql):
        """
        spark引擎执行sql语句
        :param sql: 
        :return: 
        """
        sql_to_execute = sql.format(**self.params_dict)
        self.spark.sql(sql_to_execute)

    def return_df(self, sql):
        """
        spark引擎执行sql语句并返回dataframe
        :param
        sql:
        :return:
        df
        """
        sql_to_execute = sql.format(**self.params_dict)
        df = self.spark.sql(sql_to_execute)
        return df


def date_trunc(interval, date_str):
    """
    截断到指定精度，返回相应的日期字符串
    :param interval: ['week', 'month', 'year']
    :param date_str: 
    :return: after_trunc_date_str
    """
    date_obj = parse(date_str)
    # date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    if interval == 'week':
        res = date_obj - timedelta(days=(date_obj.isocalendar()[2] - 1))
    elif interval == 'month':
        res = datetime.date(date_obj.year, date_obj.month, 1)
    elif interval == 'year':
        res = datetime.date(date_obj.year, 1, 1)
    else:
        raise Exception("interval must be ['week', 'month', 'year']")
    return res.strftime('%Y-%m-%d')


def extract(interval, date_str):
    """
    返回所取日期的子域
    :param interval: ['day', 'weekday', 'week', 'month', 'year']
    :param date_str: 
    :return: int
    """
    date_obj = parse(date_str)
    if interval == 'day':
        res = date_obj.day
    elif interval == 'weekday':
        # 周的情况返回周几的数字，
        res = date_obj.isocalendar()[2]
    elif interval == 'week':
        # 返回日期的自然周数
        res = date_obj.isocalendar()[1]
    elif interval == 'month':
        res = date_obj.month
    elif interval == 'year':
        res = date_obj.year
    else:
        raise Exception("interval must be ['day', 'weekday', 'week', 'month', 'year']")
    return res


if __name__ == '__main__':
    print(extract('week', '2018-09-09'))
    print(date_trunc('week', '2017-01-01'))
    foo = SparkInit('ym_test')
    foo.spark.sql("select udf_date_trunc('week', '2018-09-12')").show()
