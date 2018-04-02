package uw.mydb.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.conf.MydbConfig;
import uw.mydb.conf.MydbConfigManager;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * mysql组服务的管理器。
 *
 * @author axeon
 */
public class MySqlGroupManager {

    private static final Logger logger = LoggerFactory.getLogger(MySqlGroupManager.class);

    /**
     * 当前启动状态.
     */
    private static final AtomicBoolean STATE = new AtomicBoolean(false);

    /**
     * 配置文件
     */
    private static MydbConfig config = MydbConfigManager.getConfig();

    /**
     * mysql集群列表。
     */
    private static Map<String, MySqlGroupService> mysqlGroupServiceMap = new HashMap<>();

    /**
     * 根据mysqlGroupName获得对应的mysqlGroupService。
     *
     * @param mysqlGroupName
     * @return
     */
    public static MySqlGroupService getMysqlGroupService(String mysqlGroupName) {
        return mysqlGroupServiceMap.get(mysqlGroupName);
    }

    public static Map<String, MySqlGroupService> getMysqlGroupServiceMap() {
        return mysqlGroupServiceMap;
    }

    /**
     * 初始化。
     */
    public static void init() {
        for (Map.Entry<String, MydbConfig.MysqlGroupConfig> kv : config.getMysqlGroups().entrySet()) {
            MySqlGroupService service = new MySqlGroupService(kv.getValue());
            service.init();
            mysqlGroupServiceMap.put(kv.getKey(), service);
        }
    }

    /**
     * 启动mysql集群检查线程。
     */
    public static boolean start() {
        if (STATE.compareAndSet(false, true)) {
            MySqlMaintenanceService.start();
            for (MySqlGroupService groupService : mysqlGroupServiceMap.values()) {
                groupService.start();
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * 停止mysql集群。
     */
    public static boolean stop() {
        if (STATE.compareAndSet(true, false)) {
            for (MySqlGroupService groupService : mysqlGroupServiceMap.values()) {
                groupService.stop();
            }
            MySqlMaintenanceService.stop();
            return true;
        } else {
            return false;
        }
    }


}
