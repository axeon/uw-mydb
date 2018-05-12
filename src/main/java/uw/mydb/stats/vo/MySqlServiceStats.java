package uw.mydb.stats.vo;

import uw.mydb.mysql.MySqlService;

/**
 * mysql服务统计数据。
 *
 * @author axeon
 */
public class MySqlServiceStats {

    private MySqlService mySqlService;

    public MySqlServiceStats(MySqlService mySqlService) {
        this.mySqlService = mySqlService;
    }

    /**
     * 所有连接数。
     */
    public int getTotalConnections() {
        return mySqlService.getTotalSessions();
    }

    /**
     * 空闲连接数。
     *
     * @return
     */
    public int getIdleConnections() {
        return mySqlService.getIdleSessions();
    }

    /**
     * 等候线程数。
     */
    public int getAwaitingThreads() {
        return mySqlService.getAwaitingThreads();
    }


}
