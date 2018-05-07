package uw.mydb.stats.vo;

import uw.mydb.proxy.ProxySessionManager;

/**
 * MyDb统计数据。
 */
public class MyDbStats {

    /**
     * 获得当前连接数。
     *
     * @return
     */
    public int getTotalConnections() {
        return ProxySessionManager.getCount();
    }
}
