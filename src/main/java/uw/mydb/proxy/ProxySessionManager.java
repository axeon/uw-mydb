package uw.mydb.proxy;

import java.util.concurrent.ConcurrentHashMap;

/**
 * ProxySession管理器。
 *
 * @author axeon
 */
public class ProxySessionManager {

    private static ConcurrentHashMap<String, ProxyMysqlSession> map = new ConcurrentHashMap();

    /**
     * 获得在线计数。
     *
     * @return
     */
    public static int getCount() {
        return map.size();
    }

    /**
     * 获得map实例。
     *
     * @return
     */
    public static ConcurrentHashMap<String, ProxyMysqlSession> getMap() {
        return map;
    }

    /**
     * 增加一个session。
     *
     * @param key
     * @param session
     */
    public static void put(String key, ProxyMysqlSession session) {
        map.put(key, session);
    }

    /**
     * 移除一个session。
     *
     * @param key
     */
    public static void remove(String key) {
        map.remove(key);
    }
}
