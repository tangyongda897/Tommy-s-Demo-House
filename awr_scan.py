import threading
import time
import  cx_Oracle

def scan_awr(v_ins_info,v_conpool=None):


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
    v_ins_id = v_ins_info['instance_id']
    global  awr_connPool
    #创建Oracle连接
    tns = cx_Oracle.makedsn(v_ins_info['ip'], v_ins_info['listener_port'], v_ins_info['sid'])
    try:
        db1=cx_Oracle.connect(v_ins_info['username'],v_ins_info['password'],tns)
        if db1.version[:3] == '9.2':
            return

        cur1=db1.cursor()
        v_sql="SELECT *                                                         " + \
              "  FROM (  SELECT snap_id,                                        " + \
              "                 cast(BEGIN_INTERVAL_TIME as date) AS BEGIN_DATE," + \
              "                 cast(END_INTERVAL_TIME as date) AS END_DATE,    " + \
              "                 LAG (snap_id, 1) OVER (ORDER BY snap_id)        " + \
              "            FROM V$instance a, dba_hist_snapshot b               " + \
              "           WHERE a.instance_number = b.instance_number           " + \
              "        ORDER BY snap_id DESC)                                   " + \
              " WHERE ROWNUM < 2                                                "

        cur1.execute(v_sql)
        snaps=cur1.fetall()

        for snap in snaps:
            v_end_snapid=snap[0]
            v_begin_date=snap[1]
            v_end_date = snap[2]
            v_begin_snapid=snap[3]

        if v_begin_snapid is None or v_end_snapid is None:
            cur1.close()
            db1.close()
            return

        # 监控写入数据的游标
        v_conn = awr_connPool.get_conn()
        v_cur = v_conn.cursor()

        # 检查当前t_awr_perm_metric中，是否是已经分析过的数据
        # 检查时间有可能比AWR快照时间快
        v_cur.execute("select count(1) "
                      "  from t_awr_perm_metric "
                      " where inst_id = :inst_id "
                      "   and snap_id = :snap_id",
                      inst_id=v_ins_id, snap_id=v_end_snapid)
        cnt=v_cur.fetchone()

        if cnt>0:
            v_conn.close()
            cur1.close()
            v_cur.close()
            db1.close()
            return
        try:


    except cx_Oracle.DatabaseError as msg:
        print(msg)








def get_awr_metric_info(v_inst_info,v_conn=None):
    v_commit;