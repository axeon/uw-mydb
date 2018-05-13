package uw.mydb.stats.vo;

import uw.mydb.proxy.ProxyMysqlSession;
import uw.mydb.proxy.ProxySessionManager;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.counting;

/**
 * MyDb运行信息。
 *
 * @author axeon
 */
public class ServerRunInfo {

    /**
     * 返回jvm内存占用数组。
     * 格式为：maxMemory/totalMemory/freeMemory
     *
     * @return
     */
    public long[] getJvmMemory() {
        Runtime runtime = Runtime.getRuntime();
        return new long[]{runtime.maxMemory(), runtime.totalMemory(), runtime.freeMemory()};
    }

    /**
     * 获得当前连接数。
     *
     * @return
     */
    public int getTotalConnections() {
        return ProxySessionManager.getCount();
    }

    /**
     * 获得链接映射表。
     *
     * @return
     */
    public Map<String, Long> getConnectionMap() {
        return ProxySessionManager.getMap().values().stream().map(ProxyMysqlSession::getHost).collect(Collectors.groupingBy(Function.identity(), counting()));
    }

}
