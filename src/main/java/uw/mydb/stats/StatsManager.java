package uw.mydb.stats;

import uw.mydb.conf.MydbConfig;
import uw.mydb.conf.MydbConfigManager;
import uw.mydb.mysql.MySqlGroupManager;
import uw.mydb.stats.vo.*;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 统计的工厂类。
 *
 * @author axeon
 */
public class StatsManager {

    private static MydbConfig.Stats config = MydbConfigManager.getConfig().getStats();

    /**
     * 服务器统计表。
     */
    private static SqlStatsPair serverSqlStats = new SqlStatsPair(config.isServerMetrics(), config.getMetricService().isEnabled());

    /**
     * 基于客户端的统计表。
     */
    private static Map<String, SqlStatsPair> clientSqlStatsMap = new ConcurrentHashMap();

    /**
     * 基于mydb库表的统计表。
     */
    private static Map<String, SqlStatsPair> schemaSqlStatsMap = new ConcurrentHashMap();

    /**
     * 基于mysql的统计表，用于程序内统计。
     */
    private static Map<String, SqlStats> mysqlStatsMap = new ConcurrentHashMap();

    /**
     * 基于mysql库表的统计表，用于metric统计。
     */
    private static Map<String, SqlStats> mysqlDbStatsMap = new ConcurrentHashMap();


    /**
     * 获得server Sql统计。
     *
     * @return
     */
    public static SqlStatsPair getServerSqlStats() {
        return serverSqlStats;
    }

    /**
     * 获得客户端sql统计。
     *
     * @return
     */
    public static Map<String, SqlStatsPair> getClientSqlStatsMap() {
        return clientSqlStatsMap;
    }

    /**
     * 获得schema统计。
     *
     * @return
     */
    public static Map<String, SqlStatsPair> getSchemaSqlStatsMap() {
        return schemaSqlStatsMap;
    }

    /**
     * 获得mysql统计。
     *
     * @return
     */
    public static Map<String, SqlStats> getMysqlSqlStatsMap() {
        return mysqlStatsMap;
    }


    /**
     * 统计来自mydb的数据。
     */
    public static final void statsMydb(StatsInfo stats) {
        statsMydb(stats.info1, stats.info2, stats.info3, stats.isMasterSql, stats.isExeSuccess, stats.exeTime, stats.dataRowsCount, stats.affectRowsCount, stats.sendBytes, stats.recvBytes);
    }

    /**
     * 统计来自mydb的数据。
     */
    public static final void statsMydb(String clientIp, String schema, String table, boolean isMasterSql, boolean isExeSuccess, long exeTime, int dataRowsCount, int affectRowsCount, long sendBytes, long recvBytes) {
        //获得客户端统计表
        SqlStatsPair csp = null;
        if (config.isClientMetrics()) {
            csp = clientSqlStatsMap.computeIfAbsent(clientIp, s -> new SqlStatsPair(config.isClientMetrics(), config.getMetricService().isEnabled()));
        }
        //获得schema统计表。
        SqlStatsPair ssp = null;
        if (config.isSchemaMetrics()) {
            ssp = schemaSqlStatsMap.computeIfAbsent(new StringBuilder(36).append(schema).append('.').append(table).toString(), s -> new SqlStatsPair(config.isSchemaMetrics(), config.getMetricService().isEnabled()));
        }

        if (isMasterSql) {
            serverSqlStats.addSqlWriteCount(1);
            if (csp != null) {
                csp.addSqlWriteCount(1);
            }
            if (ssp != null) {
                ssp.addSqlWriteCount(1);
            }
        } else {
            serverSqlStats.addSqlReadCount(1);
            if (csp != null) {
                csp.addSqlReadCount(1);
            }
            if (ssp != null) {
                ssp.addSqlReadCount(1);
            }
        }
        if (isExeSuccess) {
            serverSqlStats.addExeSuccessCount(1);
            if (csp != null) {
                csp.addExeSuccessCount(1);
            }
            if (ssp != null) {
                ssp.addExeSuccessCount(1);
            }
        } else {
            serverSqlStats.addExeFailureCount(1);
            if (csp != null) {
                csp.addExeSuccessCount(1);
            }
            if (ssp != null) {
                ssp.addExeSuccessCount(1);
            }
        }
        serverSqlStats.addExeTime(exeTime);
        serverSqlStats.addDataRowsCount(dataRowsCount);
        serverSqlStats.addAffectRowsCount(affectRowsCount);
        serverSqlStats.addSendBytes(sendBytes);
        serverSqlStats.addRecvBytes(recvBytes);

        if (csp != null) {
            csp.addExeTime(exeTime);
            csp.addDataRowsCount(dataRowsCount);
            csp.addAffectRowsCount(affectRowsCount);
            csp.addSendBytes(sendBytes);
            csp.addRecvBytes(recvBytes);
        }

        if (ssp != null) {
            ssp.addExeTime(exeTime);
            ssp.addDataRowsCount(dataRowsCount);
            ssp.addAffectRowsCount(affectRowsCount);
            ssp.addSendBytes(sendBytes);
            ssp.addRecvBytes(recvBytes);
        }
    }

    /**
     * 统计来源于mysql的数据。
     */
    public static final void statsMysql(StatsInfo stats) {
        statsMysql(stats.info1, stats.info2, stats.info3, stats.isMasterSql, stats.isExeSuccess, stats.exeTime, stats.dataRowsCount, stats.affectRowsCount, stats.sendBytes, stats.recvBytes);
    }

    /**
     * 统计来源于mysql的数据。
     */
    public static final void statsMysql(String mysqlGroup, String mysql, String database, boolean isMasterSql, boolean isExeSuccess, long exeTime, int dataRowsCount, int affectRowsCount, long sendBytes, long recvBytes) {
        if (config.isMysqlMetrics()) {
            StringBuilder mysqlHost = new StringBuilder(100).append(mysqlGroup).append('$').append(mysql);
            //获得mysql统计表
            SqlStats msp = mysqlStatsMap.computeIfAbsent(mysqlHost.toString(), s -> new SqlStats());
            if (isMasterSql) {
                msp.addSqlWriteCount(1);
            } else {
                msp.addSqlReadCount(1);
            }
            if (isExeSuccess) {
                msp.addExeSuccessCount(1);
            } else {
                msp.addExeFailureCount(1);
            }
            msp.addExeTime(exeTime);
            msp.addDataRowsCount(dataRowsCount);
            msp.addAffectRowsCount(affectRowsCount);
            msp.addSendBytes(sendBytes);
            msp.addRecvBytes(recvBytes);

            if (config.getMetricService().isEnabled()) {
                String mysqlDb = mysqlHost.append(mysqlHost).append('$').append(database).toString();
                SqlStats mdsp = mysqlDbStatsMap.computeIfAbsent(mysqlDb, x -> new SqlStats());
                if (isMasterSql) {
                    mdsp.addSqlWriteCount(1);
                } else {
                    mdsp.addSqlReadCount(1);
                }
                if (isExeSuccess) {
                    mdsp.addExeSuccessCount(1);
                } else {
                    mdsp.addExeFailureCount(1);
                }
                mdsp.addExeTime(exeTime);
                mdsp.addDataRowsCount(dataRowsCount);
                mdsp.addAffectRowsCount(affectRowsCount);
                mdsp.addSendBytes(sendBytes);
                mdsp.addRecvBytes(recvBytes);
            }
        }
    }

    /**
     * 统计慢sql。
     */
    public static void statsSlowSql(String client, String schema, String sql, int routeSize, int rowsCount, long sendBytes, long recvBytes, long exeTime, long exeDate) {
        if (exeTime > config.getSlowQueryTimeout()) {
            SlowSql slowSql = new SlowSql(client, schema, sql, routeSize, rowsCount, sendBytes, recvBytes, exeTime, exeDate);
            //FIXME axeon@2018/7/12  此处发送slowSql。
        }
    }

    /**
     * 返回mydb服务状态。
     *
     * @return
     */
    public static ServerRunInfo getServerRunStats() {
        //获得按主机分组统计的map。
        return new ServerRunInfo();
    }

    /**
     * 获得mysql服务器状态表。
     */
    public static Map<String, MySqlRunInfo> getMySqlServiceStats() {
        ArrayList<MySqlRunInfo> list = new ArrayList<>();
        MySqlGroupManager.getMysqlGroupServiceMap().values().stream().forEach(s -> {
            s.getMasterServices().stream().forEach(x -> list.add(new MySqlRunInfo(x)));
            s.getSlaveServices().stream().forEach(x -> list.add(new MySqlRunInfo(x)));
        });
        return list.stream().collect(Collectors.toMap(MySqlRunInfo::getName, Function.identity()));
    }


}
