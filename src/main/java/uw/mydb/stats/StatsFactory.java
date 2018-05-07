package uw.mydb.stats;

import uw.mydb.stats.vo.SlowSql;
import uw.mydb.stats.vo.SqlStatsPair;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 统计的工厂类。
 *
 * @author axeon
 */
public class StatsFactory {

    private static SqlStatsPair myDbStats = new SqlStatsPair();

    /**
     * 基于客户端的统计表。
     */
    private static Map<String, SqlStatsPair> clientStatsMap = new ConcurrentHashMap();

    /**
     * 基于mydb库表的统计表。
     */
    private static Map<String, SqlStatsPair> schemaStatsMap = new ConcurrentHashMap();

    /**
     * 基于mysql库表的统计表。
     */
    private static Map<String, SqlStatsPair> mysqlStatsMap = new ConcurrentHashMap();

    /**
     * 统计来自mydb的数据。
     */
    public static void statsMydb(String clientIp, String schema, String table, boolean isMasterSql, boolean isExeSuccess, long exeTime, int dataRowsCount, int affectRowsCount, long sendBytes, long recvBytes) {
        //获得客户端统计表
        SqlStatsPair csp = clientStatsMap.putIfAbsent(clientIp, new SqlStatsPair());
        //获得schema统计表。
        SqlStatsPair ssp = schemaStatsMap.putIfAbsent(new StringBuilder().append(schema).append('.').append(table).toString(), new SqlStatsPair());

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
    public static void statsMysql(String mysqlGroup, String mysql, boolean isMasterSql, boolean isExeSuccess, long exeTime, int dataRowsCount, int affectRowsCount, long sendBytes, long recvBytes) {
        //获得mysql统计表
        SqlStatsPair msp = mysqlStatsMap.putIfAbsent(new StringBuilder().append(mysqlGroup).append('#').append(mysql).toString(), new SqlStatsPair());
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
    }

    /**
     * 统计慢sql。
     */
    public static void statsSlowSql(String client, String schema, String sql, long exeTime, long exeDate) {
        SlowSql slowSql = new SlowSql(client, schema, sql, exeTime, exeDate);
    }

}
