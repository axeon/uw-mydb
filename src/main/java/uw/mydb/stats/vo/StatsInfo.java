package uw.mydb.stats.vo;

/**
 * 基础的统计信息。
 *
 * @author axeon
 */
public class StatsInfo {

    public String info1;
    public String info2;
    public String info3;
    public boolean isMasterSql;
    public boolean isExeSuccess;
    public long exeTime;
    public int dataRowsCount;
    public int affectRowsCount;
    public long sendBytes;
    public long recvBytes;

    public StatsInfo(String info1, String info2, String info3, boolean isMasterSql, boolean isExeSuccess, long exeTime, int dataRowsCount, int affectRowsCount, long sendBytes, long recvBytes) {
        this.info1 = info1;
        this.info2 = info2;
        this.info3 = info3;
        this.isMasterSql = isMasterSql;
        this.isExeSuccess = isExeSuccess;
        this.exeTime = exeTime;
        this.dataRowsCount = dataRowsCount;
        this.affectRowsCount = affectRowsCount;
        this.sendBytes = sendBytes;
        this.recvBytes = recvBytes;
    }

}
