import threading
import time
import cx_Oracle
from config import Oracle, mylog
from datetime import datetime

logging = mylog('PWD' + datetime.now().strftime("%Y-%m-%d"), '../log/', 'awr Logging')

def start_get_password(v_awr_interval, v_awr_timeout):
    global awr_connPool
    global awr_interval
    global awr_timeout

    awr_interval = v_awr_interval
    awr_timeout = v_awr_timeout
    awr_connPool = Oracle(min=2, max=10, module='scan_awr')

    l_conn = awr_connPool.get_conn()
    l_cur = l_conn.cursor()
    try:
        # 维护t_awr_perm_metric表的准确性

        l_cur.execute("select b.id, a.mon_user, a.mon_password, b.ip, b.listener_port, b.sid "
                      "  from t_db a, t_instance b"
                      " where a.id = b.db_id "
                      "   and a.db_type = 'ORACLE' "
                      "   and a.is_gather = 1 "
                      "   and b.is_gather = 1 ")
        instances = l_cur.fetchall()
    finally:
        l_cur.close()
        l_conn.close()

    awr_sleep = round((awr_interval - awr_timeout - 20) / (len(instances) + 1), 3)
    logging.info('Start ScanAWR Thread, start interval : %f' % awr_sleep)
    # 创建线程
    for inst in instances:
        inst_info = dict()
        inst_info['instance_id'] = inst[0]
        inst_info['username'] = inst[1]
        inst_info['password'] = inst[2]
        inst_info['ip'] = inst[3]
        inst_info['listener_port'] = inst[4]
        inst_info['sid'] = inst[5]
        # 创建线程
        d = GetPassword(inst_info, awr_timeout)
        d.daemon = True
        d.start()
        time.sleep(awr_sleep)

    logging.info('ScanAWR MainThread Start Sleep : %d second.' % awr_sleep)
    time.sleep(awr_sleep)


class GetPassword(threading.Thread):
    def __init__(self, inst_info, timeout=1.0):
        super(GetPassword, self).__init__()
        self.inst_info = inst_info
        self.timeout = timeout

    def run(self):
        t = threading.Thread(target=get_pwd, args=(self.inst_info,))
        t.setDaemon(True)
        t.start()
        t.join(self.timeout)


def get_pwd(v_ins_info, v_conpool=None):
    # ----------------------------
    # v_ins_info结构
    # {
    #   'instance_id' : xxx
    #   'username' : xxx
    #   'password' : xxx
    #   'ip' : xxxx
    #   'listener_port' : xxx
    #   'sid' : xxx
    #   'bigtran_level' : xxx
    # }
    # ---------------------------
    global awr_connPool

    v_ins_id = v_ins_info['instance_id']
    # 创建Oracle连接
    tns = cx_Oracle.makedsn(v_ins_info['ip'], v_ins_info['listener_port'], v_ins_info['sid'])
    try:
        db1 = cx_Oracle.connect(v_ins_info['username'], v_ins_info['password'], tns)
        if db1.version[:3] == '9.2':
            return

        cur1 = db1.cursor()
        v_sql = "SELECT a.username, a.account_status, a.profile,                               " + \
                "       b.password, b.ptime ,(select instance_name from V$instance)  as instance_name  " + \
                "  FROM dba_users a, sys.user$ b      " + \
                " WHERE a.account_status='OPEN'       " + \
                "   AND a.user_id = b.user#           ";

        cur1.execute(v_sql)
        users = cur1.fetall()

        for user in users:
            v_username = user[0]
            v_password = user[3]
            v_inst_name = user[5]
            v_conn = awr_connPool.get_conn()
            v_cur = v_conn.cursor()
            v_cur.execute("insert into tmp_pwd values(:username,:password,:instance_name)", username=v_username,
                          password=v_password, instance_name=v_inst_name)
            v_cur.commit()
            v_cur.close()


    except cx_Oracle.DatabaseError as msg:
        print(msg)
