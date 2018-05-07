package uw.mydb.metric;

import java.util.concurrent.atomic.AtomicInteger;

public class MyDbStats extends BaseSqlStats {

    /**
     * 所有连接数。
     */
    protected AtomicInteger totalConnections = new AtomicInteger();

    public long getTotalConnections() {
        return totalConnections.get();
    }

    public void setTotalConnections(int totalConnections) {
        this.totalConnections.set(totalConnections);
    }

    public long getAndClearTotalConnections() {
        return totalConnections.getAndSet(0);
    }


}
