# coding=utf8

import cx_Oracle
from multiprocessing import Process
import time
from datetime import datetime
from config import DATABASES, mylog
from GetPassword import *

# from scan_redis import start_redis_dispatch

logging = mylog('realtime-' + datetime.now().strftime("%Y-%m-%d"), '../log/')
g_dispatch_rate = dict()

# ----------------------------------------------------------------------------------------------#
#      #     #     #     ###  #     #        ######   #######   #####   ###  #     #
#      ##   ##    # #     #   ##    #        #     #  #        #     #   #   ##    #
#      # # # #   #   #    #   # #   #        #     #  #        #         #   # #   #
#      #  #  #  #     #   #   #  #  #        ######   #####    #  ####   #   #  #  #
#      #     #  #######   #   #   # #        #     #  #        #     #   #   #   # #
#      #     #  #     #   #   #    ##        #     #  #        #     #   #   #    ##
#      #     #  #     #  ###  #     #        ######   #######   #####   ###  #     #
# ----------------------------------------------------------------------------------------------#
if __name__ == '__main__':

    dms = dict()
    db = cx_Oracle.connect(DATABASES.get('user'), DATABASES.get('password'), DATABASES.get('dsn'))
    cur = db.cursor()
    cur.execute("select method, interval, timeout "
                "  from c_dispatch_method "
                " where class = 'realtime' ")
    rows = cur.fetchall()
    for row in rows:
        v_func_name = row[0]
        v_interval = row[1]
        v_timeout = row[2]
        v_prev_time = time.mktime(datetime.now().timetuple()) - 100000  # 第一次执行即调用
        dms[v_func_name] = [v_interval, v_timeout, v_prev_time]
    db.close()

    logging.info("Start Call " + "abc")
    p = Process(target=start_get_password, args=(1000, 200))
    p.daemon = True
    p.start()