package uw.mydb.metric.vo;

/**
 * sql统计数据对。
 * 里面包含两个sql统计对象，一个是全局对象，一个是临时对象。
 *
 * @author axeon
 */
public class SqlStatsPair {

    public SqlStats sqlStats = new SqlStats();

    public SqlStats sqlStatsTemp = new SqlStats();


    public void addSqlReadCount(long sqlCount) {
        sqlStats.addSqlReadCount(sqlCount);
        sqlStatsTemp.addSqlReadCount(sqlCount);
    }

    public void addSqlWriteCount(long sqlCount) {
        sqlStats.addSqlWriteCount(sqlCount);
        sqlStatsTemp.addSqlWriteCount(sqlCount);

    }

    public void addExeSuccessCount(long exeSuccessCount) {
        sqlStats.addExeSuccessCount(exeSuccessCount);
        sqlStatsTemp.addExeSuccessCount(exeSuccessCount);

    }

    public void addExeFailureCount(long exeFailureCount) {
        sqlStats.addExeFailureCount(exeFailureCount);
        sqlStatsTemp.addExeFailureCount(exeFailureCount);

    }

    public void addDataRowsCount(long dataRowsCount) {
        sqlStats.addDataRowsCount(dataRowsCount);
        sqlStatsTemp.addDataRowsCount(dataRowsCount);

    }

    public void addAffectRowsCount(long affectRowsCount) {
        sqlStats.addAffectRowsCount(affectRowsCount);
        sqlStatsTemp.addAffectRowsCount(affectRowsCount);

    }

    public void addExeTime(long exeTime) {
        sqlStats.addExeTime(exeTime);
        sqlStatsTemp.addExeTime(exeTime);

    }

    public void addSendBytes(long sendBytes) {
        sqlStats.addSendBytes(sendBytes);
        sqlStatsTemp.addSendBytes(sendBytes);

    }

    public void addRecvBytes(long recvBytes) {
        sqlStats.addRecvBytes(recvBytes);
        sqlStats.addRecvBytes(recvBytes);

    }

    public long getSqlReadCount() {
        return sqlStats.getSqlReadCount();
    }

    public long getAndClearSqlReadCount() {
        return sqlStatsTemp.getAndClearSqlReadCount();
    }

    public long getSqlWriteCount() {
        return sqlStats.getSqlWriteCount();
    }

    public long getAndClearSqlWriteCount() {
        return sqlStatsTemp.getAndClearSqlWriteCount();
    }

    public long getDataRowsCount() {
        return sqlStats.getDataRowsCount();
    }

    public long getAndClearDataRowsCount() {
        return sqlStatsTemp.getAndClearDataRowsCount();
    }

    public long getAffectRowsCount() {
        return sqlStats.getAffectRowsCount();
    }

    public long getAndClearAffectRowsCount() {
        return sqlStatsTemp.getAndClearAffectRowsCount();
    }

    public long getExeSuccessCount() {
        return sqlStats.getExeSuccessCount();
    }

    public long getAndClearExeSuccessCount() {
        return sqlStatsTemp.getAndClearExeSuccessCount();
    }

    public long getAndClearExeFailureCount() {
        return sqlStatsTemp.getAndClearExeFailureCount();
    }

    public long getExeFailureCount() {
        return sqlStats.getExeFailureCount();
    }

    public long getExeTime() {
        return sqlStats.getExeTime();
    }

    public long getAndClearExeTime() {
        return sqlStatsTemp.getAndClearExeTime();
    }

    public long getSendBytes() {
        return sqlStats.getSendBytes();
    }

    public long getAndClearSendBytes() {
        return sqlStatsTemp.getAndClearSendBytes();
    }

    public long getRecvBytes() {
        return sqlStats.getRecvBytes();
    }

    public long getAndClearRecvBytes() {
        return sqlStatsTemp.getAndClearRecvBytes();
    }
}
