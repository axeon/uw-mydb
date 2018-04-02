package uw.mydb.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.conf.MydbConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 用来维护一个mysql配置组。
 *
 * @author axeon
 */
public class MySqlGroupService {

    private static final Logger logger = LoggerFactory.getLogger(MySqlGroupService.class);

    private static final int MAX_RETRY_TIMES = 30;

    /**
     * 当前启动状态.
     */
    private final AtomicBoolean status = new AtomicBoolean(false);


    /**
     * mysql组配置信息。
     */
    private MydbConfig.MysqlGroupConfig config;

    /**
     * 主库列表。
     */
    private List<MySqlService> masterServices = new ArrayList<>();

    /**
     * 从库列表。
     */
    private List<MySqlService> slaveServices = new ArrayList<>();

    /**
     * 读写服务列表，利用weight做了重复数据。
     */
    private List<MySqlService> allServices = new ArrayList<>();

    /**
     * 获得master索引位置。
     */
    private int masterIndex;

    /**
     * 获得slave索引位置。
     */
    private int slaveIndex;

    /**
     * 所有列表
     */
    private int allIndex;

    /**
     * 默认构造器。
     *
     * @param config
     */
    public MySqlGroupService(MydbConfig.MysqlGroupConfig config) {
        this.config = config;
    }

    /**
     * 获得组服务名。
     *
     * @return
     */
    public String getName() {
        return config.getName();
    }

    /**
     * 初始化。
     */
    public void init() {
        List<MydbConfig.MysqlConfig> mList = config.getMasters();
        for (MydbConfig.MysqlConfig config : mList) {
            MySqlService mysqlService = new MySqlService(this, config);
            mysqlService.setSlaveNode(false);
            masterServices.add(mysqlService);
            //权重设置
            if (config.getWeight() > 9) {
                config.setWeight(9);
            }
            for (int k = 0; k < config.getWeight(); k++) {
                allServices.add(mysqlService);
            }
            //初始化
            mysqlService.init();
        }

        List<MydbConfig.MysqlConfig> sList = config.getSlaves();
        for (MydbConfig.MysqlConfig config : sList) {
            MySqlService mysqlService = new MySqlService(this, config);
            mysqlService.setSlaveNode(true);
            slaveServices.add(mysqlService);
            //权重设置
            if (config.getWeight() > 9) {
                config.setWeight(9);
            }
            for (int k = 0; k < config.getWeight(); k++) {
                allServices.add(mysqlService);
            }
            //初始化
            mysqlService.init();
        }
    }

    /**
     * 启动服务。
     *
     * @return
     */
    public boolean start() {
        if (status.compareAndSet(false, true)) {
            for (MySqlService mysqlService : masterServices) {
                mysqlService.start();
            }
            for (MySqlService mysqlService : slaveServices) {
                mysqlService.start();
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * 关闭服务。
     *
     * @return
     */
    public boolean stop() {
        if (status.compareAndSet(true, false)) {
            for (MySqlService mysqlService : masterServices) {
                mysqlService.stop();
            }
            for (MySqlService mysqlService : slaveServices) {
                mysqlService.stop();
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * 得到当前用于写的MySQLService
     */
    public MySqlService getMasterService() {
        MySqlService service = null;
        for (int i = 0; i < MAX_RETRY_TIMES; i++) {
            masterIndex++;
            service = masterServices.get(masterIndex % masterServices.size());
            if (service.isAlive()) {
                break;
            }
        }
        return service;
    }


    /**
     * 得到当前用于写的MySQLService
     */
    public MySqlService getSlaveService() {
        MySqlService service = null;
        for (int i = 0; i < MAX_RETRY_TIMES; i++) {
            slaveIndex++;
            service = slaveServices.get(slaveIndex % slaveServices.size());
            if (service.isAlive()) {
                break;
            }
        }
        return service;
    }

    /**
     * 得到当前用于读的MySQLService（负载均衡模式，如果支持）
     */
    public MySqlService getLBReadService() {
        MySqlService service = null;
        for (int i = 0; i < MAX_RETRY_TIMES; i++) {
            allIndex++;
            service = allServices.get(allIndex % allServices.size());
            if (service.isAlive()) {
                break;
            }
        }
        return service;
    }

}
