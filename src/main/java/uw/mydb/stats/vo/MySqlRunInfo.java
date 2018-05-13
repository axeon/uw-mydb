package uw.mydb.stats.vo;

import uw.mydb.mysql.MySqlService;

/**
 * mysql服务统计数据。
 *
 * @author axeon
 */
public class MySqlRunInfo {

    private MySqlService mySqlService;

    public MySqlRunInfo(MySqlService mySqlService) {
        this.mySqlService = mySqlService;
    }

    /**
     * 获得mysql service名称。
     *
     * @return
     */
    public String getName() {
        return this.mySqlService.getGroupName() + "$" + mySqlService.getName();
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


    /**
     * 获得建立中的连接数。
     *
     * @return
     */
    public int getPendingCreateConnections() {
        return mySqlService.getPendingCreateConnections();
    }

    /**
     * 获得连接创建计数。
     *
     * @return
     */
    public int getConnectionCreateCount() {
        return mySqlService.getConnectionCreateCount();
    }

    /**
     * 获得连接创建错误计数。
     *
     * @return
     */
    public int getConnectionCreateErrorCount() {
        return mySqlService.getConnectionCreateErrorCount();
    }
}
