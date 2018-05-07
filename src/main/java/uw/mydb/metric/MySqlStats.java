package uw.mydb.metric;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * mysql服务统计数据。
 *
 * @author axeon
 */
public class MySqlStats extends BaseSqlStats {

    /**
     * 所有连接数。
     */
    protected AtomicInteger totalConnections = new AtomicInteger();

    /**
     * 活动连接数。
     */
    protected AtomicInteger activeConnections = new AtomicInteger();

    /**
     * 等候线程数。
     */
    protected AtomicInteger pendingThreads = new AtomicInteger();


    public long getTotalConnections() {
        return totalConnections.get();
    }

    public void setTotalConnections(int totalConnections) {
        this.totalConnections.set(totalConnections);
    }

    public long getAndClearTotalConnections() {
        return totalConnections.getAndSet(0);
    }

    public long getActiveConnections() {
        return activeConnections.get();
    }

    public void setActiveConnections(int totalConnections) {
        this.activeConnections.set(totalConnections);
    }

    public long getAndClearActiveConnections() {
        return activeConnections.getAndSet(0);
    }

    public long getPendingThreads() {
        return pendingThreads.get();
    }

    public void setPendingThreads(int totalConnections) {
        this.pendingThreads.set(totalConnections);
    }

    public long getAndClearPendingThreads() {
        return pendingThreads.getAndSet(0);
    }


}
