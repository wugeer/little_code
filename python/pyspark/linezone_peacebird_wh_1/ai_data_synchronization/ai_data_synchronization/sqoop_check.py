# -*- coding: utf-8 -*-

# ----------------------------------------------------------------------------
# Project: linezone_peacebird_wh_1
# Filename: sqoop_check
# Author: zsm
# Date: 2018/11/12 11:35
# ----------------------------------------------------------------------------

import sys
from pandas import DataFrame
from ai_data_synchronization.utils.publictool import OracleConnection
from ai_data_synchronization.utils.tools import query_day_date


def check_data(cursor):
    p_input_date = query_day_date(sys.argv[1], 1, "%Y-%m-%d")
    params = {}
    params['p_input_date'] = p_input_date
    params['task_type_name'] = '补货单据'
    check_sql = """
        select 
            TASKTYPENAME
            , STATUS
        from NEANDS3.TOC_STATUS
        where 
            TOCDATE = '{p_input_date}'
            and TASKTYPENAME= '{task_type_name}'
        """.format(**params)
    s = cursor.execute(check_sql)
    db_data = cursor.fetchall()
    if db_data:
        info = db_data[0]
        print('info:{}'.format(info))
        status = info[1]
        if status == '1':
            print('Flag is True with status code {}'.format(status))
            return True
        else:
            print('flag is False with status code {}'.format(status))
            sys.exit(1)
    else:
        print('flag is None')
        sys.exit(1)

def main():
    conn = OracleConnection().get_connection()
    cursor = OracleConnection().get_cursor()
    check_data(cursor)


if __name__ == '__main__':
    main()
