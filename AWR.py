# -*- coding: utf-8 -*-
"""
Copyright (c) 2018—2019 Beijing Haitian Horizon Technology Service Corporation. All rights reserved.

扫描AWR中的性能指标模块
频率 1小时1次
"""

from config import Oracle, mylog
import cx_Oracle
import time
import threading
from datetime import datetime

logging = mylog('normal-' + datetime.now().strftime("%Y-%m-%d"), '../log/', 'awr Logging')


def start_awr_dispatch(v_awr_interval, v_awr_timeout):
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
        l_cur.execute("delete from t_awr_perm_metric "
                      " where inst_id in "
                      "       (select cc.inst_id "
                      "          from (select b.id from t_db a, t_instance b "
                      "                 where a.id = b.db_id "
                      "                   and a.is_gather = 1 "
                      "                   and b.is_gather = 1 ) aa, "
                      "               t_awr_perm_metric cc "
                      "         where aa.id(+) = cc.inst_id "
                      "           and aa.id is null)")
        l_conn.commit()
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
#创建线程
    for inst in instances:
        inst_info = dict()
        inst_info['instance_id'] = inst[0]
        inst_info['username'] = inst[1]
        inst_info['password'] = inst[2]
        inst_info['ip'] = inst[3]
        inst_info['listener_port'] = inst[4]
        inst_info['sid'] = inst[5]
        #创建线程
        d = ScanAwrDeamon(inst_info, awr_timeout)
        d.daemon = True
        d.start()
        time.sleep(awr_sleep)

    logging.info('ScanAWR MainThread Start Sleep : %d second.' % awr_sleep)
    time.sleep(awr_sleep)


# -------------------------------------
#  调 度 Oracle 扫 描 AWR 检 测 线 程
# -------------------------------------
class ScanAwrDeamon(threading.Thread):
    def __init__(self, inst_info, timeout=1.0):
        super(ScanAwrDeamon, self).__init__()
        self.inst_info = inst_info
        self.timeout = timeout

    def run(self):
        t = threading.Thread(target=scan_awr, args=(self.inst_info,))
        t.setDaemon(True)
        t.start()
        t.join(self.timeout)


#########################
# awr扫描监控
#########################
# v_connPool : JOB模块调用awr分析时，传入连接池
def scan_awr(v_ins_info, v_connPool=None):
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
    # print("inst_id : %d" % v_ins_id)
    global awr_connPool
    if v_connPool is not None:
        awr_connPool = v_connPool

    tns = cx_Oracle.makedsn(v_ins_info['ip'], v_ins_info['listener_port'], v_ins_info['sid'])
    try:
        db1 = cx_Oracle.connect(v_ins_info['username'], v_ins_info['password'], tns)
        if db1.version[:3] == '9.2':
            db1.close()
            return

        cur1 = db1.cursor()

        # 计算最近的两次AWR快照

        v_sql = "select snap_id, begin_date, end_date, prev_snap_id " + \
                "  from (with aa as (select a.dbid, " + \
                "                           a.instance_number, " + \
                "                           lag(snap_id, 1) over(PARTITION BY a.dbid, " + \
                "                                                a.instance_number order by snap_id) prev_snap_id, " + \
                "                           lag(end_interval_time, 1) over(PARTITION BY a.dbid, " + \
                "                                              a.instance_number order by snap_id) prev_snap_date, " + \
                "                           a.snap_id, " + \
                "                           a.begin_interval_time as begin_snap_date, " + \
                "                           a.end_interval_time as end_snap_date " + \
                "                      from dba_hist_snapshot a, v$instance b " + \
                "                     where a.instance_number = b.instance_number " + \
                "                       and a.begin_interval_time > b.startup_time " + \
                "                       and a.end_interval_time > b.startup_time " + \
                "                       and a.begin_interval_time > sysdate - 12 / 24 " + \
                "                       and a.end_interval_time > sysdate - 12 / 24 " + \
                "                     order by snap_id, instance_number) " + \
                "         select snap_id, " + \
                "                cast(begin_snap_date as date) begin_date, " + \
                "                cast(end_snap_date as date) end_date, " + \
                "                prev_snap_id " + \
                "           from aa " + \
                "          where aa.prev_snap_date is not null " + \
                "            and aa.prev_snap_date = aa.begin_snap_date " + \
                "          order by snap_id desc) " + \
                "          where rownum < 2 "
        cur1.execute(v_sql)
        snaps = cur1.fetchall()
    except cx_Oracle.DatabaseError as msg:
        logging.debug('inst_id %d | %s' % (v_ins_id, str(msg)))
        if 'db1' in locals():
            db1.close()
        return

    v_begin_snap_date = None
    v_end_snap_date = None
    v_begin_snap_id = None
    v_end_snap_id = None

    # 计算最近两次的AWR snap_id
    for snap in snaps:
        v_end_snap_id = snap[0]
        v_begin_snap_date = snap[1]
        v_end_snap_date = snap[2]
        v_begin_snap_id = snap[3]

    # 如果没检查到AWR快照，后面则不做了，在t_awr_perm_metric没有最近的信息，或者sample_date不更新
    if v_begin_snap_id is None or v_end_snap_id is None:
        if 'db1' in locals():
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
                  inst_id=v_ins_id, snap_id=v_end_snap_id)
    flag_count = v_cur.fetchone()[0]
    if flag_count == 1:
        cur1.close()
        db1.close()
        v_cur.close()
        v_conn.close()
        return

    try:
        # 写入数据库性能指标
        get_awr_perform_metric(v_ins_id, cur1, v_begin_snap_id, v_end_snap_id, v_begin_snap_date, v_end_snap_date, v_cur)
        v_conn.commit()

        # 写入TopSQL - 执行时间维度
        get_awr_topsql_et(v_ins_id, cur1, v_begin_snap_id, v_end_snap_id, v_end_snap_date, v_cur)
        v_conn.commit()

        # 写入TopSQL - 逻辑读维度
        get_awr_topsql_bg(v_ins_id, cur1, v_begin_snap_id, v_end_snap_id, v_end_snap_date, v_cur)
        v_conn.commit()

        # 写入TopSQL - CPU维度
        get_awr_topsql_cpu(v_ins_id, cur1, v_begin_snap_id, v_end_snap_id, v_end_snap_date, v_cur)
        v_conn.commit()

    except cx_Oracle.DatabaseError as msg:
        logging.error('inst_id : %d | %s' % (v_ins_id, msg), exc_info=1)
    finally:
        cur1.close()
        db1.close()
        v_cur.close()
        v_conn.close()


###########################
# 获取并写入AWR数据库性能指标
###########################
def get_awr_perform_metric(v_ins_id, cur1, v_begin_snap_id, v_end_snap_id, v_begin_snap_date, v_end_snap_date, v_cur):

    v_transactions = None
    v_commit = None
    v_rollback = None
    v_physical_reads = None
    v_physical_writes = None
    v_physical_writes_d = None
    v_physical_writes_fc = None
    v_logical_reads = None
    v_consistent_gets = None
    v_db_block_gets = None
    v_redo_size = None
    v_dfsr_sequential = None
    v_dfsr_scattered = None
    v_hard_parse_count = None
    v_executions_count = None
    v_lfpw = None
    v_dfpw = None

    # 查询状态指标
    v_sql = "select b.stat_name, " + \
            "       round((e.value - b.value) / " + \
            "            ((cast(es.end_interval_time as date) - " + \
            "              cast(bs.end_interval_time as date)) * 86400), 1) as sec_value " + \
            "  from dba_hist_sysstat b, " + \
            "       dba_hist_sysstat e, " + \
            "       dba_hist_snapshot  bs, " + \
            "       dba_hist_snapshot es, " + \
            "       v$instance i " + \
            " where b.snap_id = :begin_snap_id " + \
            "   and e.snap_id = :end_snap_id " + \
            "   and b.instance_number = i.instance_number " + \
            "   and e.instance_number = i.instance_number " + \
            "   and b.instance_number = bs.instance_number " + \
            "   and e.instance_number = es.instance_number " + \
            "   and b.snap_id = bs.snap_id " + \
            "   and e.snap_id = es.snap_id " + \
            "   and b.stat_name in ('user commits', 'user rollbacks', 'physical reads', " + \
            "                       'physical writes direct', 'physical writes from cache'," + \
            "                       'consistent gets', 'db block gets', 'redo size', " + \
            "                       'parse count (hard)', 'execute count') " + \
            "   and e.stat_name in ('user commits', 'user rollbacks', 'physical reads', " + \
            "                       'physical writes direct', 'physical writes from cache'," + \
            "                       'consistent gets', 'db block gets', 'redo size', " + \
            "                       'parse count (hard)', 'execute count') " + \
            "   and b.stat_name = e.stat_name "
    cur1.execute(v_sql, begin_snap_id=v_begin_snap_id, end_snap_id=v_end_snap_id)
    stats = cur1.fetchall()

    for stat in stats:
        v_stat_name = stat[0]
        if v_stat_name == 'user commits':
            v_commit = stat[1]
        elif v_stat_name == 'user rollbacks':
            v_rollback = stat[1]
        elif v_stat_name == 'physical reads':
            v_physical_reads = stat[1]
        elif v_stat_name == 'physical writes direct':
            v_physical_writes_d = stat[1]
        elif v_stat_name == 'physical writes from cache':
            v_physical_writes_fc = stat[1]
        elif v_stat_name == 'consistent gets':
            v_consistent_gets = stat[1]
        elif v_stat_name == 'db block gets':
            v_db_block_gets = stat[1]
        elif v_stat_name == 'redo size':
            v_redo_size = stat[1]
        elif v_stat_name == 'parse count (hard)':
            v_hard_parse_count = stat[1]
        elif v_stat_name == 'execute count':
            v_executions_count = stat[1]

    if (v_physical_writes_d is not None) and (v_physical_writes_fc is not None):
        v_physical_writes = v_physical_writes_d + v_physical_writes_fc
    if (v_consistent_gets is not None) and (v_db_block_gets is not None):
        v_logical_reads = v_consistent_gets + v_db_block_gets
    if (v_commit is not None) and (v_rollback is not None):
        v_transactions = v_commit + v_rollback

    # 采集等待事件指标
    v_sql = "select b.event_name, " + \
            "       (e.total_waits - b.total_waits) as waits, " + \
            "       trunc((e.time_waited_micro - b.time_waited_micro) / 1000000) as time_s, " + \
            "       round((e.time_waited_micro - b.time_waited_micro) / 1000 / " + \
            "             decode((e.total_waits - b.total_waits),0,1, " + \
            "                    (e.total_waits - b.total_waits))) as avg_wait_ms " + \
            "  from dba_hist_system_event b, dba_hist_system_event e, v$instance i " + \
            " where b.snap_id = :begin_snap_id " + \
            "   and e.snap_id = :end_snap_id " + \
            "   and b.instance_number = i.instance_number " + \
            "   and e.instance_number = i.instance_number " + \
            "   and b.event_name = e.event_name " + \
            "   and b.event_name in " + \
            "       ('db file sequential read', 'db file scattered read', 'log file parallel write', " + \
            "        'db file parallel write') " + \
            "   and e.event_name in " + \
            "       ('db file sequential read', 'db file scattered read', 'log file parallel write', " + \
            "        'db file parallel write') "
    cur1.execute(v_sql, begin_snap_id=v_begin_snap_id, end_snap_id=v_end_snap_id)
    events = cur1.fetchall()

    for event in events:
        v_event_name = event[0]
        if v_event_name == 'db file sequential read':
            v_dfsr_sequential = event[3]
        elif v_event_name == 'db file scattered read':
            v_dfsr_scattered = event[3]
        elif v_event_name == 'log file parallel write':
            v_lfpw = event[3]
        elif v_event_name == 'db file parallel write':
            v_dfpw = event[3]

    v_cur.execute("merge into t_awr_perm_metric a "
                  "using (select :id as id from dual) b "
                  "   on (a.inst_id = b.id) "
                  " when MATCHED then "
                  "      update set a.snap_id = :snap_id, "
                  "                 a.begin_snap_date = :begin_snap_date, "
                  "                 a.end_snap_date = :end_snap_date, "
                  "                 a.dfsr_sequential = :dfsr_sequential, "
                  "                 a.dfsr_scattered = :dfsr_scattered, "
                  "                 a.transactions = :transactions, "
                  "                 a.commits = :commits, "
                  "                 a.rollbacks = :rollbacks, "
                  "                 a.physical_reads = :physical_reads, "
                  "                 a.physical_writes = :physical_writes,"
                  "                 a.logical_reads = :logical_reads, "
                  "                 a.redo_size = :redo_size, "
                  "                 a.hard_parse = :hard_parse, "
                  "                 a.executions = :executions, "
                  "                 a.sample_date = sysdate, "
                  "                 a.lfpw = :lfpw, "
                  "                 a.dfpw = :dfpw "
                  " when NOT MATCHED then "
                  "      insert(inst_id, snap_id, begin_snap_date, end_snap_date, dfsr_sequential, "
                  "             dfsr_scattered, transactions, commits, rollbacks, physical_reads, "
                  "             physical_writes, logical_reads, redo_size, hard_parse, executions, sample_date, "
                  "             lfpw, dfpw) "
                  "      values(:id, :snap_id, :begin_snap_date, :end_snap_date, :dfsr_sequential, "
                  "             :dfsr_scattered, :transactions, :commits, :rollbacks, :physical_reads, "
                  "             :physical_writes, :logical_reads, :redo_size, :hard_parse, :executions, sysdate, "
                  "             :lfpw, :dfpw)",
                  id=v_ins_id, snap_id=v_end_snap_id, begin_snap_date=v_begin_snap_date,
                  end_snap_date=v_end_snap_date, dfsr_sequential=v_dfsr_sequential, dfsr_scattered=v_dfsr_scattered,
                  transactions=v_transactions, commits=v_commit, rollbacks=v_rollback,
                  physical_reads=v_physical_reads, physical_writes=v_physical_writes, logical_reads=v_logical_reads,
                  redo_size=v_redo_size, hard_parse=v_hard_parse_count, executions=v_executions_count,
                  lfpw=v_lfpw, dfpw=v_dfpw)
    v_cur.execute("insert into t_awr_perm_metric_his(inst_id, snap_id, begin_snap_date, end_snap_date, "
                  "        dfsr_sequential, dfsr_scattered, transactions, commits, rollbacks, "
                  "        physical_reads, physical_writes, logical_reads, redo_size, "
                  "        hard_parse, executions, sample_date, lfpw, dfpw) "
                  "    values(:inst_id, :snap_id, :begin_snap_date, :end_snap_date, "
                  "           :dfsr_sequential, :dfsr_scattered, :transactions, :commits, :rollbacks, "
                  "           :physical_reads, :physical_writes, :logical_reads, :redo_size, "
                  "           :hard_parse, :executions, sysdate, :lfpw, :dfpw)",
                  inst_id=v_ins_id, snap_id=v_end_snap_id, begin_snap_date=v_begin_snap_date,
                  end_snap_date=v_end_snap_date, dfsr_sequential=v_dfsr_sequential, dfsr_scattered=v_dfsr_scattered,
                  transactions=v_transactions, commits=v_commit, rollbacks=v_rollback,
                  physical_reads=v_physical_reads, physical_writes=v_physical_writes, logical_reads=v_logical_reads,
                  redo_size=v_redo_size, hard_parse=v_hard_parse_count, executions=v_executions_count,
                  lfpw=v_lfpw, dfpw=v_dfpw)


###########################
# 计算TopSQL - 执行时间维度
###########################
def get_awr_topsql_et(v_ins_id, cur1, v_begin_snap_id, v_end_snap_id, v_end_snap_date,  v_cur):
    # 采集topSQL，通过执行时间
    v_sql = "select sql_id, module, elapsed_time, executions, elapsed_per_exec, " + \
            "       cpu_time, cpu_per_exec, gets_per_exec, pct, buffer_gets " + \
            " from ( " + \
            "  with dt as ( " + \
            "    select (e.value - b.value) as db_time " + \
            "      from dba_hist_sys_time_model e, dba_hist_sys_time_model b, v$database db, v$instance i " + \
            "     where b.snap_id = :begin_snap_id " + \
            "       and e.snap_id = :end_snap_id " + \
            "       and b.dbid = db.dbid " + \
            "       and e.dbid = db.dbid " + \
            "       and b.instance_number = i.instance_number " + \
            "       and e.instance_number = i.instance_number " + \
            "       and e.stat_name = 'DB time' " + \
            "       and b.stat_name = 'DB time'), " + \
            "  sqlt as ( " + \
            "    select sql_id, " + \
            "           max(a.module) module, " + \
            "           sum(a.elapsed_time_delta) elap, " + \
            "           sum(a.cpu_time_delta) cput, " + \
            "           sum(a.executions_delta) exec, " + \
            "           sum(a.buffer_gets_delta) gets " + \
            "      from dba_hist_sqlstat a, v$instance i " + \
            "     where a.instance_number = i.instance_number " + \
            "       and snap_id = :end_snap_id " + \
            "     group by sql_id) " + \
            "  select sqlt.sql_id, sqlt.module,  " + \
            "         round(sqlt.elap / 1000000, 1) as elapsed_time,  " + \
            "         sqlt.exec as executions,  " + \
            "         round(sqlt.elap/decode(sqlt.exec,0,1,nvl(sqlt.exec,1))/1000000, 2) as elapsed_per_exec,  " + \
            "         round(sqlt.cput / 1000000, 1) as cpu_time, " + \
            "         round(sqlt.cput/decode(sqlt.exec,0,1,nvl(sqlt.exec,1))/1000000, 2) as cpu_per_exec,   " + \
            "         round(sqlt.gets/decode(sqlt.exec,0,1,nvl(sqlt.exec,1)), 1) as gets_per_exec, " + \
            "         round((sqlt.elap / dt.db_time) * 100, 1) as pct, " + \
            "         sqlt.gets as buffer_gets  " + \
            "    from sqlt, dt " + \
            "   order by sqlt.elap desc nulls last " + \
            ") where rownum < 31 "
    cur1.execute(v_sql, begin_snap_id=v_begin_snap_id, end_snap_id=v_end_snap_id)
    topsqls = cur1.fetchall()

    # ----------------------------------------------------------------------------
    # 向监控库写入数据
    # ----------------------------------------------------------------------------
    v_cur.execute("delete from t_awr_topsql_etp where inst_id = :inst_id ", inst_id=v_ins_id)
    v_cur.execute("insert into t_awr_topsql_etp "
                  "  (inst_id, "
                  "   snap_id, "
                  "   snap_date, "
                  "   sql_id, "
                  "   module, "
                  "   executions, "
                  "   elapsed_time, "
                  "   elapsed_per_exec, "
                  "   cpu_time, "
                  "   cpu_per_exec, "
                  "   gets, "
                  "   gets_per_exec, "
                  "   percent, "
                  "   sample_date) "
                  "  select inst_id, "
                  "         snap_id, "
                  "         snap_date, "
                  "         sql_id, "
                  "         module, "
                  "         executions, "
                  "         elapsed_time, "
                  "         elapsed_per_exec, "
                  "         cpu_time, "
                  "         cpu_per_exec, "
                  "         gets, "
                  "         gets_per_exec, "
                  "         percent, "
                  "         sample_date "
                  "    from t_awr_topsql_et "
                  "   where inst_id = :inst_id ", inst_id=v_ins_id)
    v_cur.execute("delete from t_awr_topsql_et where inst_id = :inst_id ", inst_id=v_ins_id)

    for topsql in topsqls:
        v_sql_id = topsql[0]
        v_module = topsql[1]
        v_elapsed_time = topsql[2]
        v_executions = topsql[3]
        v_elapsed_per_exec = topsql[4]
        v_cpu_time = topsql[5]
        v_cpu_per_exec = topsql[6]
        v_gets_per_exec = topsql[7]
        v_pct = topsql[8]
        v_gets = topsql[9]
        v_cur.execute("insert into t_awr_topsql_et "
                      "  (inst_id, "
                      "   snap_id, "
                      "   snap_date, "
                      "   sql_id, "
                      "   module, "
                      "   executions, "
                      "   elapsed_time, "
                      "   elapsed_per_exec, "
                      "   cpu_time, "
                      "   cpu_per_exec, "
                      "   gets, "
                      "   gets_per_exec, "
                      "   percent, "
                      "   sample_date) "
                      " values(:inst_id, "
                      "        :snap_id, "
                      "        :snap_date, "
                      "        :sql_id, "
                      "        :module, "
                      "        :executions, "
                      "        :elapsed_time, "
                      "        :elapsed_per_exec, "
                      "        :cpu_time, "
                      "        :cpu_per_exec, "
                      "        :gets, "
                      "        :gets_per_exec, "
                      "        :percent, "
                      "        sysdate) ",
                      inst_id=v_ins_id, snap_id=v_end_snap_id, snap_date=v_end_snap_date,
                      sql_id=v_sql_id, module=v_module, executions=v_executions, elapsed_time=v_elapsed_time,
                      elapsed_per_exec=v_elapsed_per_exec, cpu_time=v_cpu_time, cpu_per_exec=v_cpu_per_exec,
                      gets=v_gets, gets_per_exec=v_gets_per_exec,
                      percent=v_pct)

    v_cur.callproc('p_ai_analyze.p_topsql_etai_analyze', [v_ins_id])


###########################
# 计算TopSQL - 逻辑读维度
###########################
def get_awr_topsql_bg(v_ins_id, cur1, v_begin_snap_id, v_end_snap_id, v_end_snap_date,  v_cur ):
    # 采集TopSQL, 通过逻辑读维度
    v_sql = "select sql_id, module, elapsed_time, executions, elapsed_per_exec, " + \
            "       cpu_time, cpu_per_exec, buffer_gets, gets_per_exec, pct " + \
            " from ( " + \
            "  with dt as " + \
            "   (select sum(e.value - b.value) as gets " + \
            "      from dba_hist_sysstat  b, " + \
            "           dba_hist_sysstat  e, " + \
            "           dba_hist_snapshot bs, " + \
            "           dba_hist_snapshot es, " + \
            "           v$instance        i " + \
            "     where b.snap_id = :begin_snap_id " + \
            "       and e.snap_id = :end_snap_id " + \
            "       and b.instance_number = i.instance_number " + \
            "       and e.instance_number = i.instance_number " + \
            "       and b.instance_number = bs.instance_number " + \
            "       and e.instance_number = es.instance_number " + \
            "       and b.snap_id = bs.snap_id " + \
            "       and e.snap_id = es.snap_id " + \
            "       and b.stat_name in ('consistent gets', 'db block gets') " + \
            "       and e.stat_name in ('consistent gets', 'db block gets') " + \
            "       and b.stat_name = e.stat_name), " + \
            "  sqlt as " + \
            "   (select sql_id, " + \
            "           max(a.module) module, " + \
            "           sum(a.elapsed_time_delta) elap, " + \
            "           sum(a.cpu_time_delta) cput, " + \
            "           sum(a.executions_delta) exec, " + \
            "           sum(a.buffer_gets_delta) gets " + \
            "      from dba_hist_sqlstat a, v$instance i " + \
            "     where a.instance_number = i.instance_number " + \
            "       and snap_id = :end_snap_id " + \
            "     group by sql_id) " + \
            "  select sqlt.sql_id, " + \
            "         sqlt.module, " + \
            "         round(sqlt.elap / 1000000, 1) as elapsed_time, " + \
            "         sqlt.exec as executions, " + \
            "         round(sqlt.elap / decode(sqlt.exec, 0, 1, nvl(sqlt.exec, 1)) / " + \
            "               1000000, " + \
            "               2) as elapsed_per_exec, " + \
            "         round(sqlt.cput / 1000000, 1) as cpu_time, " + \
            "         round(sqlt.cput / decode(sqlt.exec, 0, 1, nvl(sqlt.exec, 1)) / " + \
            "               1000000, " + \
            "               2) as cpu_per_exec, " + \
            "         sqlt.gets as buffer_gets, " + \
            "         round(sqlt.gets / decode(sqlt.exec, 0, 1, nvl(sqlt.exec, 1)), 1) as gets_per_exec, " + \
            "         round((sqlt.gets / dt.gets) * 100, 1) as pct " + \
            "    from sqlt, dt " + \
            "   order by sqlt.gets desc nulls last " + \
            ") where rownum < 31 "
    cur1.execute(v_sql, begin_snap_id=v_begin_snap_id, end_snap_id=v_end_snap_id)
    bg_topsqls = cur1.fetchall()

    # ---------------------------------------------------------------
    # 向监控库写入TopSQL
    v_cur.execute("delete from t_awr_topsql_bgp where inst_id = :inst_id ", inst_id=v_ins_id)
    v_cur.execute("insert into t_awr_topsql_bgp "
                  "  (inst_id, "
                  "   snap_id, "
                  "   snap_date, "
                  "   sql_id, "
                  "   module, "
                  "   executions, "
                  "   elapsed_time, "
                  "   elapsed_per_exec, "
                  "   cpu_time, "
                  "   cpu_per_exec, "
                  "   gets, "
                  "   gets_per_exec, "
                  "   percent, "
                  "   sample_date) "
                  "  select inst_id, "
                  "         snap_id, "
                  "         snap_date, "
                  "         sql_id, "
                  "         module, "
                  "         executions, "
                  "         elapsed_time, "
                  "         elapsed_per_exec, "
                  "         cpu_time, "
                  "         cpu_per_exec, "
                  "         gets, "
                  "         gets_per_exec, "
                  "         percent, "
                  "         sample_date "
                  "    from t_awr_topsql_bg "
                  "   where inst_id = :inst_id ", inst_id=v_ins_id)
    v_cur.execute("delete from t_awr_topsql_bg where inst_id = :inst_id ", inst_id=v_ins_id)
    for topsql in bg_topsqls:
        v_sql_id = topsql[0]
        v_module = topsql[1]
        v_elapsed_time = topsql[2]
        v_executions = topsql[3]
        v_elapsed_per_exec = topsql[4]
        v_cpu_time = topsql[5]
        v_cpu_per_exec = topsql[6]
        v_gets = topsql[7]
        v_gets_per_exec = topsql[8]
        v_pct = topsql[9]

        v_cur.execute("insert into t_awr_topsql_bg "
                      "  (inst_id, "
                      "   snap_id, "
                      "   snap_date, "
                      "   sql_id, "
                      "   module, "
                      "   executions, "
                      "   elapsed_time, "
                      "   elapsed_per_exec, "
                      "   cpu_time, "
                      "   cpu_per_exec, "
                      "   gets, "
                      "   gets_per_exec, "
                      "   percent, "
                      "   sample_date) "
                      " values(:inst_id, "
                      "        :snap_id, "
                      "        :snap_date, "
                      "        :sql_id, "
                      "        :module, "
                      "        :executions, "
                      "        :elapsed_time, "
                      "        :elapsed_per_exec, "
                      "        :cpu_time, "
                      "        :cpu_per_exec, "
                      "        :gets, "
                      "        :gets_per_exec, "
                      "        :percent, "
                      "        sysdate) ",
                      inst_id=v_ins_id, snap_id=v_end_snap_id, snap_date=v_end_snap_date,
                      sql_id=v_sql_id, module=v_module, executions=v_executions, elapsed_time=v_elapsed_time,
                      elapsed_per_exec=v_elapsed_per_exec, cpu_time=v_cpu_time, cpu_per_exec=v_cpu_per_exec,
                      gets=v_gets, gets_per_exec=v_gets_per_exec,
                      percent=v_pct)

    v_cur.callproc('p_ai_analyze.p_topsql_bgai_analyze', [v_ins_id])


###########################
# 计算TopSQL - CPU维度
###########################
def get_awr_topsql_cpu(v_ins_id, cur1, v_begin_snap_id, v_end_snap_id, v_end_snap_date, v_cur):
    v_sql = "select sql_id, module, elapsed_time, executions, elapsed_per_exec, " + \
            "       cpu_time, cpu_per_exec, buffer_gets, gets_per_exec, pct " + \
            " from ( " + \
            "  with dt as " + \
            "   (select nvl(e.value - b.value, 1) as cput " + \
            "      from dba_hist_sys_time_model e, " + \
            "           dba_hist_sys_time_model b, " + \
            "           v$database              db, " + \
            "           v$instance              i " + \
            "     where b.snap_id = :begin_snap_id " + \
            "       and e.snap_id = :end_snap_id " + \
            "       and b.dbid = db.dbid " + \
            "       and e.dbid = db.dbid " + \
            "       and b.instance_number = i.instance_number " + \
            "       and e.instance_number = i.instance_number " + \
            "       and e.stat_name = 'DB CPU' " + \
            "       and b.stat_name = 'DB CPU'), " + \
            "  sqlt as " + \
            "   (select sql_id, " + \
            "           max(a.module) module, " + \
            "           sum(a.elapsed_time_delta) elap, " + \
            "           sum(a.cpu_time_delta) cput, " + \
            "           sum(a.executions_delta) exec, " + \
            "           sum(a.buffer_gets_delta) gets " + \
            "      from dba_hist_sqlstat a, v$instance i " + \
            "     where a.instance_number = i.instance_number " + \
            "       and snap_id = :end_snap_id " + \
            "     group by sql_id) " + \
            "  select sqlt.sql_id, " + \
            "         sqlt.module, " + \
            "         round(sqlt.elap / 1000000, 1) as elapsed_time, " + \
            "         sqlt.exec as executions, " + \
            "         round(sqlt.elap / decode(sqlt.exec, 0, 1, nvl(sqlt.exec, 1)) / " + \
            "               1000000, " + \
            "               2) as elapsed_per_exec, " + \
            "         round(sqlt.cput / 1000000, 1) as cpu_time, " + \
            "         round(sqlt.cput / decode(sqlt.exec, 0, 1, nvl(sqlt.exec, 1)) / " + \
            "               1000000, " + \
            "               2) as cpu_per_exec, " + \
            "         sqlt.gets as buffer_gets, " + \
            "         round(sqlt.gets / decode(sqlt.exec, 0, 1, nvl(sqlt.exec, 1)), 1) as gets_per_exec, " + \
            "         round((sqlt.cput / dt.cput) * 100, 1) as pct " + \
            "    from sqlt, dt " + \
            "   order by sqlt.cput desc nulls last " + \
            ") where rownum < 31 "

    cur1.execute(v_sql, begin_snap_id=v_begin_snap_id, end_snap_id=v_end_snap_id)
    cpu_topsqls = cur1.fetchall()

    # ---------------------------------------------------------------
    # 向监控库写入TopSQL
    v_cur.execute("delete from t_awr_topsql_cpup where inst_id = :inst_id ", inst_id=v_ins_id)
    v_cur.execute("insert into t_awr_topsql_cpup "
                  "  (inst_id, "
                  "   snap_id, "
                  "   snap_date, "
                  "   sql_id, "
                  "   module, "
                  "   executions, "
                  "   elapsed_time, "
                  "   elapsed_per_exec, "
                  "   cpu_time, "
                  "   cpu_per_exec, "
                  "   gets, "
                  "   gets_per_exec, "
                  "   percent, "
                  "   sample_date) "
                  "  select inst_id, "
                  "         snap_id, "
                  "         snap_date, "
                  "         sql_id, "
                  "         module, "
                  "         executions, "
                  "         elapsed_time, "
                  "         elapsed_per_exec, "
                  "         cpu_time, "
                  "         cpu_per_exec, "
                  "         gets, "
                  "         gets_per_exec, "
                  "         percent, "
                  "         sample_date "
                  "    from t_awr_topsql_cpu "
                  "   where inst_id = :inst_id ", inst_id=v_ins_id)
    v_cur.execute("delete from t_awr_topsql_cpu where inst_id = :inst_id ", inst_id=v_ins_id)
    for topsql in cpu_topsqls:
        v_sql_id = topsql[0]
        v_module = topsql[1]
        v_elapsed_time = topsql[2]
        v_executions = topsql[3]
        v_elapsed_per_exec = topsql[4]
        v_cpu_time = topsql[5]
        v_cpu_per_exec = topsql[6]
        v_gets = topsql[7]
        v_gets_per_exec = topsql[8]
        v_pct = topsql[9]

        v_cur.execute("insert into t_awr_topsql_cpu "
                      "  (inst_id, "
                      "   snap_id, "
                      "   snap_date, "
                      "   sql_id, "
                      "   module, "
                      "   executions, "
                      "   elapsed_time, "
                      "   elapsed_per_exec, "
                      "   cpu_time, "
                      "   cpu_per_exec, "
                      "   gets, "
                      "   gets_per_exec, "
                      "   percent, "
                      "   sample_date) "
                      " values(:inst_id, "
                      "        :snap_id, "
                      "        :snap_date, "
                      "        :sql_id, "
                      "        :module, "
                      "        :executions, "
                      "        :elapsed_time, "
                      "        :elapsed_per_exec, "
                      "        :cpu_time, "
                      "        :cpu_per_exec, "
                      "        :gets, "
                      "        :gets_per_exec, "
                      "        :percent, "
                      "        sysdate) ",
                      inst_id=v_ins_id, snap_id=v_end_snap_id, snap_date=v_end_snap_date,
                      sql_id=v_sql_id, module=v_module, executions=v_executions, elapsed_time=v_elapsed_time,
                      elapsed_per_exec=v_elapsed_per_exec, cpu_time=v_cpu_time, cpu_per_exec=v_cpu_per_exec,
                      gets=v_gets, gets_per_exec=v_gets_per_exec,
                      percent=v_pct)

    v_cur.callproc('p_ai_analyze.p_topsql_cpuai_analyze', [v_ins_id])
