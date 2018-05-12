package uw.mydb.stats;

import uw.mydb.conf.MydbConfig;
import uw.mydb.conf.MydbConfigManager;
import uw.mydb.mysql.MySqlGroupManager;
import uw.mydb.stats.vo.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 统计的工厂类。
 *
 * @author axeon
 */
public class StatsFactory {

    private static MydbConfig.Stats config = MydbConfigManager.getConfig().getStats();

    /**
     * 服务器统计表。
     */
    private static SqlStatsPair myDbStats = new SqlStatsPair(config.isServerMetrics(), config.getMetricService().isEnabled());

    /**
     * 基于客户端的统计表。
     */
    private static Map<String, SqlStatsPair> clientStatsMap = new ConcurrentHashMap();

    /**
     * 基于mydb库表的统计表。
     */
    private static Map<String, SqlStatsPair> schemaStatsMap = new ConcurrentHashMap();

    /**
     * 基于mysql的统计表，用于程序内统计。
     */
    private static Map<String, SqlStats> mysqlStatsMap = new ConcurrentHashMap();

    /**
     * 基于mysql库表的统计表，用于metric统计。
     */
    private static Map<String, SqlStats> mysqlDbStatsMap = new ConcurrentHashMap();

    public static SqlStatsPair getMyDbStats() {
        return myDbStats;
    }

    public static Map<String, SqlStatsPair> getClientStatsMap() {
        return clientStatsMap;
    }

    public static Map<String, SqlStatsPair> getSchemaStatsMap() {
        return schemaStatsMap;
    }

    /**
     * 获得mysql库统计。
     *
     * @return
     */
    public static Map<String, SqlStats> getMysqlDbStatsMap() {
        return mysqlStatsMap;
    }

    /**
     * 统计来自mydb的数据。
     */
    public static void statsMydb(String clientIp, String schema, String table, boolean isMasterSql, boolean isExeSuccess, long exeTime, int dataRowsCount, int affectRowsCount, long sendBytes, long recvBytes) {
        //获得客户端统计表
        SqlStatsPair csp = clientStatsMap.putIfAbsent(clientIp, new SqlStatsPair(config.isClientMetrics(), config.getMetricService().isEnabled()));
        //获得schema统计表。
        SqlStatsPair ssp = schemaStatsMap.putIfAbsent(new StringBuilder().append(schema).append('.').append(table).toString(), new SqlStatsPair(config.isSchemaMetrics(), config.getMetricService().isEnabled()));

        if (isMasterSql) {
            myDbStats.addSqlWriteCount(1);
            csp.addSqlWriteCount(1);
            ssp.addSqlWriteCount(1);
        } else {
            myDbStats.addSqlReadCount(1);
            csp.addSqlReadCount(1);
            ssp.addSqlReadCount(1);
        }
        if (isExeSuccess) {
            myDbStats.addExeSuccessCount(1);
            csp.addExeSuccessCount(1);
            ssp.addExeSuccessCount(1);
        } else {
            myDbStats.addExeFailureCount(1);
            csp.addExeSuccessCount(1);
            ssp.addExeSuccessCount(1);
        }
        myDbStats.addExeTime(exeTime);
        myDbStats.addDataRowsCount(dataRowsCount);
        myDbStats.addAffectRowsCount(affectRowsCount);
        myDbStats.addSendBytes(sendBytes);
        myDbStats.addRecvBytes(recvBytes);

        csp.addExeTime(exeTime);
        csp.addDataRowsCount(dataRowsCount);
        csp.addAffectRowsCount(affectRowsCount);
        csp.addSendBytes(sendBytes);
        csp.addRecvBytes(recvBytes);

        ssp.addExeTime(exeTime);
        ssp.addDataRowsCount(dataRowsCount);
        ssp.addAffectRowsCount(affectRowsCount);
        ssp.addSendBytes(sendBytes);
        ssp.addRecvBytes(recvBytes);
    }

    /**
     * 统计来源于mysql的数据。
     */
    public static void statsMysql(String mysqlGroup, String mysql, String database, boolean isMasterSql, boolean isExeSuccess, long exeTime, int dataRowsCount, int affectRowsCount, long sendBytes, long recvBytes) {
        String mysqlHost = new StringBuilder(100).append(mysqlGroup).append('$').append(mysql).toString();
        String mysqlDb = new StringBuilder(100).append('$').append(database).toString();
        if (config.isMysqlMetrics()) {
            //获得mysql统计表
            SqlStats msp = mysqlStatsMap.putIfAbsent(mysqlHost, new SqlStats());
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
                SqlStats mdsp = mysqlDbStatsMap.putIfAbsent(mysqlDb, new SqlStats());
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
    public static void statsSlowSql(String client, String schema, String sql, long exeTime, long exeDate) {
        if (exeTime > config.getSlowQueryTimeout()) {
            SlowSql slowSql = new SlowSql(client, schema, sql, exeTime, exeDate);
        }
    }

    /**
     * 返回mydb服务状态。
     *
     * @return
     */
    public static ServerStats getMydbServiceStats() {
        //获得按主机分组统计的map。
        return new ServerStats();
    }

    /**
     * 获得mysql服务器状态。
     */
    public static List<MySqlServiceStats> getMySqlStats() {
        ArrayList<MySqlServiceStats> list = new ArrayList<>();
        MySqlGroupManager.getMysqlGroupServiceMap().values().stream().forEach(s -> {
            s.getMasterServices().stream().forEach(x -> list.add(new MySqlServiceStats(x)));
            s.getSlaveServices().stream().forEach(x -> list.add(new MySqlServiceStats(x)));
        });
        return list;
    }

}
