package uw.mydb.stats.vo;

/**
 * mysql服务统计数据。
 *
 * @author axeon
 */
public class MySqlStats {

    /**
     * 所有连接数。
     */
    protected int totalConnections;

    /**
     * 活动连接数。
     */
    protected int activeConnections;

    /**
     * 等候线程数。
     */
    protected int pendingThreads;


    public int getTotalConnections() {
        return totalConnections;
    }

    public int getActiveConnections() {
        return activeConnections;
    }

    public int getPendingThreads() {
        return pendingThreads;
    }
}
