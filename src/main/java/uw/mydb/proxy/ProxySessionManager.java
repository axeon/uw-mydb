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
