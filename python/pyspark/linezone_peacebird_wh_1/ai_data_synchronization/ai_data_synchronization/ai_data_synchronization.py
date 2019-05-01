# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: linezone-peacebird-backend-1
# Filename: ai_data_synchronization_测试
# Author: zsm
# Date: 2018/11/2 23:32
# ----------------------------------------------------------------------------

import os
import sys
import time
import datetime
import json
import requests
import pandas as pd
from pandas import DataFrame
from ai_data_synchronization.utils.tools import query_some_monday, query_day_date, \
    if_query_nothing, get_db_data, jm_md5, insert_db_df, delete_db_data, insert_db_data
from ai_data_synchronization.utils.publictool import TaskThreadPoolExecutors
from ai_data_synchronization.config import DevelopmentConfig, AiDataSynConfig, Log
from ai_data_synchronization.constants.ResponseCodeMsgConstants import RESPONSE_CODE_MSG_CONSTANTS
from ai_data_synchronization.exceptions.AiDataSynException import GetTokenException, \
    DataSynException, SysException, QueryNothingException, CleanDataException, \
    DatabaseException


class AiDataSyn(object):

    def __init__(self):
        self.header = AiDataSynConfig.HEADER
        self.secret = AiDataSynConfig.SECRET
        self.token_url = AiDataSynConfig.TOKEN_URL
        self.ai_data_syn_url = AiDataSynConfig.AI_DATA_SYN_URL

    def get_response(self, url, data):
        data = json.dumps(data, sort_keys=True)
        # json = data,可以不用显示执行header的content_type为'application/json; charset=utf-8'
        # data将被自动转化为json格式
        response = requests.post(url, data=data, headers=self.header)
        return response.json()

    def get_token(self):
        # code = 40000, msg = '签名验证错误'
        # code = 40001, msg = 'JSON格式错误'
        response = self.get_response(self.token_url, self.secret)
        code = response.get('Code')
        msg = response.get('Msg')
        try:
            if code == 0:
                self.secret_id = response.get('SecretId')
                self.access_token = response.get('AccessToken')
            else:
                raise GetTokenException(code, msg)
        except GetTokenException as e:
            Log.token_log.error(str(e))
            raise e

    def get_signature(self):
        timestamp = str(int(round(time.time() * 1000)))
        sign_str = self.secret_id + timestamp + self.access_token
        signature = jm_md5(sign_str)
        # print(self.signature)
        return signature, timestamp

    def get_feedback(self, send_data, refno):
        response = self.get_response(self.ai_data_syn_url, send_data)
        code = response.get('Code')
        msg = response.get('Msg')
        docno = response.get('Docno')
        ai_data_syn_info = DataFrame(data=[[refno, code, msg]], columns=['refno', 'status_code', 'msg'])
        try:
            if msg == '保存成功':
                print('保存成功')
            else:
                raise DataSynException(code, msg, send_data)
        except DataSynException as e:
            Log.syn_log.error(e)
        finally:
            return ai_data_syn_info

    def clean_db_data(self, db_data):
        try:
            columns = ['send_org_code',
                       'receive_org_code',
                       'refno',
                       'SKU',
                       'QTY']
            df = DataFrame(data=db_data, columns=columns)
            df.fillna('-')
            df = df.astype(str)
            df_group = df.groupby('refno')
            send_data_list = []
            refno_list = []
            for refno, refno_df in df_group:
                c_orig_code = refno_df['send_org_code'].tolist()[0]
                c_store_code = refno_df['receive_org_code'].tolist()[0]
                # 以refno为标准判断订单是否已存在，如果已存在返回Code 40001,Msg ”外部订单已存在“
                refno = refno
                refno_info = refno_df[['QTY', 'SKU']].to_dict(orient='recodes')
                signature, timestamp = self.get_signature()
                send_data = {
                    "Data": {
                        "C_ORIG_CODE": c_orig_code,
                        "C_STORE_CODE": c_store_code,
                        "REFNO": refno,
                        "USER_CODE": "",
                        "BILLDATE": self.billdate,
                        "TOC_RET": "N",
                        "FROMTYPE": "AI",
                        "DESCRIPTION": "AI生成",
                        "LIST": refno_info
                    },
                    "SecretId": "{secret_id}".format(secret_id=self.secret_id),
                    "Timestamp": int(timestamp),
                    "Signature": "{signature}".format(signature=signature)
                }
                send_data_list.append(send_data)
                refno_list.append(refno)
        except Exception as e:
            message = RESPONSE_CODE_MSG_CONSTANTS.MESSAGE_SHOPS_INFO_CLEAN_DATA_FAILED + '<' + str(e) + '>'
            log_str = str(tuple([RESPONSE_CODE_MSG_CONSTANTS.CODE_SHOPS_INFO_CLEAN_DATA_FAILED, message]))
            Log.server_log.error(log_str)
            raise CleanDataException(
                RESPONSE_CODE_MSG_CONSTANTS.CODE_SHOPS_INFO_CLEAN_DATA_FAILED,
                message)
        else:
            return send_data_list, refno_list

    def check_exception(self, info_table_name, table_name, where_condi, params):
        check_exception_sql = """
            select 
                b.send_org_code,
                b.receive_org_code,
                b.refno,
                b.sku_code,
                b.qty
            from {info_table_name} as a
            inner join {table_name} as b
                on a.refno = b.refno
            where {where_cindi} and a.msg != '保存成功'
            """.format(info_table_name=info_table_name, table_name=table_name, where_cindi=where_condi)
        schema_config = "set search_path to {};".format(DevelopmentConfig.SQLALCHEMY_SCHEMA)
        check_exception_sql = schema_config + check_exception_sql
        exception_data = get_db_data(check_exception_sql, params)
        if len(exception_data) == 0:
            return
        else:
            send_data_list, refno_list = self.clean_db_data(exception_data)
            refno_list = tuple(refno_list)
            for i, info in enumerate(zip(send_data_list, refno_list)):
                send_data = info[0]
                refno = info[1]
                ai_data_syn_info = self.get_feedback(send_data, refno)
                if i == 0:
                    ai_data_syn_info_df = ai_data_syn_info
                else:
                    ai_data_syn_info_df = pd.concat([ai_data_syn_info_df, ai_data_syn_info], axis=0)
            ai_data_syn_info_df['type'] = table_name.split('_')[2]
            ai_data_syn_info_df['billdate'] = self.billdate
            ai_data_syn_info_df['etl_time'] = datetime.datetime.now()
            ai_data_syn_info_df = ai_data_syn_info_df[['refno', 'type', 'status_code', 'msg', 'billdate', 'etl_time']]
            # 先删除在重新推送单据范围内的单据推送信息，再插入最新的推送信息
            where_condi = 'a.refno in :refno_list'
            params['refno_list'] = refno_list
            delete_sql = """
                delete from {info_table_name} as a
                where {where_condi}
                """.format(info_table_name=info_table_name, where_condi=where_condi)
            delete_sql = schema_config + delete_sql
            delete_db_data(delete_sql, params)
            insert_db_df(info_table_name, ai_data_syn_info_df)

    def cal_exe(self, p_input_date, num):
        try:
            params = {}
            self.billdate = query_day_date(p_input_date, num, "%Y%m%d")
            params['billdate'] = self.billdate
            where_condi = 'a.billdate = :billdate'
            table_name = sys.argv[2]
            info_table_name = table_name[:(table_name.find("drp")+3)] + '_info'
            sql = """
                 select 
                     a.send_org_code,
                     a.receive_org_code,
                     a.refno,
                     a.sku_code,
                     a.qty
                 from {table_name} as a
                 where {where_condi}
                 """.format(table_name=table_name, where_condi=where_condi)
            schema_config = "set search_path to {};".format(DevelopmentConfig.SQLALCHEMY_SCHEMA)
            sql = schema_config + sql
            db_data = get_db_data(sql, params)
            # 调拨周一跑
            if table_name == 'rst_sku_allot_day_drp':
                this_monday = query_some_monday(self.billdate)
                if self.billdate == this_monday:
                    if_query_nothing(db_data)
                else:
                    return
            else:
                if_query_nothing(db_data)
            send_data_list, refno_list = self.clean_db_data(db_data)
            start_time = time.time()
            multi = TaskThreadPoolExecutors(self.get_feedback, 10, send_data_list, refno_list)
            # 得到每条单据的返回码和返回信息
            ai_data_syn_info_list = multi.result
            for i, ai_data_syn_info in enumerate(ai_data_syn_info_list):
                if i == 0:
                    ai_data_syn_info_df = ai_data_syn_info
                else:
                    ai_data_syn_info_df = pd.concat([ai_data_syn_info_df, ai_data_syn_info], axis=0)
            ai_data_syn_info_df['type'] = table_name.split('_')[2]
            ai_data_syn_info_df['billdate'] = self.billdate
            ai_data_syn_info_df['etl_time'] = datetime.datetime.now()
            ai_data_syn_info_df = ai_data_syn_info_df[['refno', 'type', 'status_code', 'msg', 'billdate', 'etl_time']]
            # 将返回码和返回信息等先删除再插入，防止重复
            delete_sql = """
                delete from {info_table_name} as a
                where {where_condi}
                """.format(info_table_name=info_table_name, where_condi=where_condi)
            delete_sql = schema_config + delete_sql
            delete_db_data(delete_sql, params)
            insert_db_df(info_table_name, ai_data_syn_info_df)
            end_time = time.time()
            print(end_time-start_time)
            time.sleep(60 * 1)
            start_time = time.time()
            self.check_exception(info_table_name, table_name, where_condi, params)
            end_time = time.time()
            print(end_time-start_time)
        except (GetTokenException, DatabaseException, QueryNothingException,
                CleanDataException, DataSynException) as e:
            raise e
        except Exception as e:
            message = RESPONSE_CODE_MSG_CONSTANTS.MESSAGE_SYS_ERROR + '<' + str(e) + '>'
            log_str = str(tuple([RESPONSE_CODE_MSG_CONSTANTS.CODE_SYS_ERROR, message]))
            Log.server_log.error(log_str)
            raise SysException(
                RESPONSE_CODE_MSG_CONSTANTS.CODE_SYS_ERROR,
                message
            )


def main():
    p_input_date = sys.argv[1]
    ai_data_syn = AiDataSyn()
    ai_data_syn.get_token()
    ai_data_syn.cal_exe(p_input_date, 1)


