#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import cx_Oracle
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from multiprocessing import cpu_count
from concurrent import futures
from ai_data_synchronization.config import DevelopmentConfig

# pg数据库
engine_str = DevelopmentConfig.SQLALCHEMY_DATABASE_URI
engine = create_engine(engine_str, encoding="utf-8")
Session = sessionmaker(bind=engine, autocommit=False, autoflush=False, expire_on_commit=False)
# oracle编码
# os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

# 连接pg数据库
@contextmanager
def open_session():
    """
    可以使用 with 上下文，在 with 结束之后自动 commit
    """
    _session = Session()
    try:
        yield _session
        _session.commit()
    except Exception as e:
        # _session.rollback()
        raise e
    finally:
        _session.close()


# 连接oracle数据库
class OracleConnection:
    def __init__(self, host=None, port=None, username=None, password=None, db_name=None, url=None):
        self.host = host if host else DevelopmentConfig.ORACLE_HOST
        self.port = port if port else DevelopmentConfig.ORACLE_HOST
        self.username = username if username else DevelopmentConfig.ORACLE_USERNAME
        self.password = password if password else DevelopmentConfig.ORACLE_PASSWD
        self.dbs = db_name if db_name else DevelopmentConfig.ORACLE_DB_NAME
        self.url = url if url else DevelopmentConfig.ORACLE_URL
        self.conn = self.connect_2_db()
        self.cursor = self.conn.cursor()

    def connect_2_db(self):
        # import os
        # os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
        # 或cx_Oracle.connect(username/password@host:post/db_name)
        # conn = cx_Oracle.connect(self.username, self.password, str(self.host) + ":" + str(self.port)+str(self.dbs))
        conn = cx_Oracle.connect(self.url)
        return conn

    def insert_sql(self, sql=None):
        self.cursor.execute(sql)
        self.commit()
        self.disconnect_2_db()

    def update_sql(self, sql=None):
        self.cursor.execute(sql)
        self.commit()
        self.disconnect_2_db()

    def disconnect_2_db(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def get_connection(self):
        return self.conn

    def get_cursor(self):
        return self.cursor

    def commit(self):
        return self.conn.commit()

    def get_engine(self):
        from sqlalchemy import create_engine
        return create_engine('oracle://{username}:{password}@{host}:{port}{dbs}'.format(
            username=self.username, password=self.password, host=self.host, port=self.port, dbs=self.dbs))


# 多线程
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

