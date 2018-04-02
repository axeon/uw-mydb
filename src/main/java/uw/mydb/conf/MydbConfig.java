package uw.mydb.conf;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.*;

/**
 * mydb配置类。.
 *
 * @author axeon
 */
@ConfigurationProperties(prefix = "uw.mydb")
public class MydbConfig {

    /**
     * 服务器配置
     */
    private ServerConfig server = new ServerConfig();

    /**
     * 用户账号设置
     */
    private Map<String, UserConfig> users = new LinkedHashMap<>();

    /**
     * mysql配置组
     */
    private Map<String, MysqlGroupConfig> mysqlGroups = new LinkedHashMap<>();

    /**
     * schemas设置
     */
    private Map<String, SchemaConfig> schemas = new LinkedHashMap<>();

    /**
     * 分表路由设置。
     */
    private Map<String, RouteConfig> routes = new LinkedHashMap<>();

    public ServerConfig getServer() {
        return server;
    }

    public void setServer(ServerConfig server) {
        this.server = server;
    }

    public Map<String, UserConfig> getUsers() {
        return users;
    }

    public void setUsers(Map<String, UserConfig> users) {
        this.users = users;
    }

    public Map<String, MysqlGroupConfig> getMysqlGroups() {
        return mysqlGroups;
    }

    public void setMysqlGroups(Map<String, MysqlGroupConfig> mysqlGroups) {
        this.mysqlGroups = mysqlGroups;
    }

    public Map<String, SchemaConfig> getSchemas() {
        return schemas;
    }

    public void setSchemas(Map<String, SchemaConfig> schemas) {
        this.schemas = schemas;
    }

    public Map<String, RouteConfig> getRoutes() {
        return routes;
    }

    public void setRoutes(Map<String, RouteConfig> routes) {
        this.routes = routes;
    }


    /**
     * 服务配置。
     */
    public static class ServerConfig {

        /**
         * 绑定的数据传输IP地址
         */
        private String ip = "0.0.0.0";
        /**
         * 绑定的数据传输端口
         */
        private int port = 8066;


        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

    }

    /**
     * 用户账号配置
     */
    public static class UserConfig {
        /**
         * 密码
         */
        private String password;

        /**
         * 可以访问的schemas列表
         */
        private List<String> schemas;

        /**
         * 允许访问的主机地址
         */
        private List<String> hosts;

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public List<String> getSchemas() {
            return schemas;
        }

        public void setSchemas(List<String> schemas) {
            this.schemas = schemas;
        }

        public List<String> getHosts() {
            return hosts;
        }

        public void setHosts(List<String> hosts) {
            this.hosts = hosts;
        }
    }

    /**
     * mysql组配置
     */
    public static class MysqlGroupConfig {

        /**
         * 名称
         */
        private String name;

        /**
         * 复制组类型
         */
        private GroupTypeEnum groupType;

        /**
         * 切换类型
         */
        private GroupSwitchTypeEnum switchType;

        /**
         * mysql主机列表
         */
        private List<MysqlConfig> masters = new ArrayList<>();

        /**
         * mysql从机列表
         */
        private List<MysqlConfig> slaves = new ArrayList<>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public GroupTypeEnum getGroupType() {
            return groupType;
        }

        public void setGroupType(GroupTypeEnum groupType) {
            this.groupType = groupType;
        }

        public GroupSwitchTypeEnum getSwitchType() {
            return switchType;
        }

        public void setSwitchType(GroupSwitchTypeEnum switchType) {
            this.switchType = switchType;
        }

        public List<MysqlConfig> getMasters() {
            return masters;
        }

        public void setMasters(List<MysqlConfig> masters) {
            this.masters = masters;
        }

        public List<MysqlConfig> getSlaves() {
            return slaves;
        }

        public void setSlaves(List<MysqlConfig> slaves) {
            this.slaves = slaves;
        }

        public enum GroupSwitchTypeEnum {
            NOT_SWITCH, SWITCH;
        }

        public enum GroupTypeEnum {
            // 单一节点
            SINGLE_NODE(GlobalConstants.SINGLE_NODE_HEARTBEAT_SQL, GlobalConstants.MYSQL_SLAVE_STATUS_COLUMNS),
            // 普通主从
            MASTER_SLAVE(GlobalConstants.MASTER_SLAVE_HEARTBEAT_SQL, GlobalConstants.MYSQL_SLAVE_STATUS_COLUMNS),
            // 普通基于garela cluster集群
            GARELA_CLUSTER(GlobalConstants.GARELA_CLUSTER_HEARTBEAT_SQL, GlobalConstants.MYSQL_CLUSTER_STATUS_COLUMNS);

            private String heartbeatSQL;
            private String[] fetchCols;

            GroupTypeEnum(String heartbeatSQL, String[] fetchCols) {
                this.heartbeatSQL = heartbeatSQL;
                this.fetchCols = fetchCols;
            }

            public String getHeartbeatSQL() {
                return heartbeatSQL;
            }

            public String[] getFetchCols() {
                return fetchCols;
            }
        }
    }

    /**
     * mysql链接配置
     */
    public static class MysqlConfig {

        /**
         * 读取权重
         */
        private int weight = 1;

        /**
         * 主机
         */
        private String host;
        /**
         * 端口号
         */
        private int port;
        /**
         * 用户名
         */
        private String user;
        /**
         * 密码
         */
        private String password;
        /**
         * 最大连接数
         */
        private int maxConn = 1000;
        /**
         * 最小连接数
         */
        private int minConn = 1;
        /**
         * 最大重试次数
         */
        private int maxRetry = GlobalConstants.MAX_RETRY_COUNT;

        /**
         * 连接闲时超时秒数.
         */
        private int connIdleTimeout = 180;

        /**
         * 连接忙时超时秒数.
         */
        private int connBusyTimeout = 180;

        /**
         * 连接最大寿命秒数.
         */
        private int connMaxAge = 1800;


        public int getWeight() {
            return weight;
        }

        public void setWeight(int weight) {
            this.weight = weight;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public int getMaxConn() {
            return maxConn;
        }

        public void setMaxConn(int maxConn) {
            this.maxConn = maxConn;
        }

        public int getMinConn() {
            return minConn;
        }

        public void setMinConn(int minConn) {
            this.minConn = minConn;
        }

        public int getMaxRetry() {
            return maxRetry;
        }

        public void setMaxRetry(int maxRetry) {
            this.maxRetry = maxRetry;
        }

        public int getConnIdleTimeout() {
            return connIdleTimeout;
        }

        public void setConnIdleTimeout(int connIdleTimeout) {
            this.connIdleTimeout = connIdleTimeout;
        }

        public int getConnBusyTimeout() {
            return connBusyTimeout;
        }

        public void setConnBusyTimeout(int connBusyTimeout) {
            this.connBusyTimeout = connBusyTimeout;
        }

        public int getConnMaxAge() {
            return connMaxAge;
        }

        public void setConnMaxAge(int connMaxAge) {
            this.connMaxAge = connMaxAge;
        }
    }

    /**
     * 虚拟数据库集群。
     */
    public static class SchemaConfig {

        /**
         * 库名
         */
        private String name;

        /**
         * 建库的sql。
         */
        private String createSql;

        /**
         * 基础库，提供库表结构参考。
         */
        private String baseNode;

        /**
         * 单独的表配置。
         */
        private Map<String, TableConfig> tables = new LinkedHashMap<>();

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getCreateSql() {
            return createSql;
        }

        public void setCreateSql(String createSql) {
            this.createSql = createSql;
        }

        public String getBaseNode() {
            return baseNode;
        }

        public void setBaseNode(String baseNode) {
            this.baseNode = baseNode;
        }

        public Map<String, TableConfig> getTables() {
            return tables;
        }

        public void setTables(Map<String, TableConfig> tables) {
            this.tables = tables;
        }
    }


    /**
     * 表配置。
     */
    public static class TableConfig {

        /**
         * 表名。
         */
        private String name;

        /**
         * 建表的sql语句。
         */
        private String createSql;

        /**
         * 路由设置。
         */
        private String route;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getCreateSql() {
            return createSql;
        }

        public void setCreateSql(String createSql) {
            this.createSql = createSql;
        }

        public String getRoute() {
            return route;
        }

        public void setRoute(String route) {
            this.route = route;
        }
    }

    /**
     * 分表规则配置。
     */
    public static class RouteConfig {

        /**
         * 分库分表名称。
         */
        private String name;

        /**
         * 分布的节点。如果是单表，可以指向单个dataNode，此时不分表。
         */
        private List<DataNodeConfig> dataNodes = new ArrayList<>();

        /**
         * 使用的算法列表。
         */
        private List<AlgorithmConfig> algorithms = new ArrayList<>();


        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<DataNodeConfig> getDataNodes() {
            return dataNodes;
        }

        public void setDataNodes(List<DataNodeConfig> dataNodes) {
            this.dataNodes = dataNodes;
        }

        public List<AlgorithmConfig> getAlgorithms() {
            return algorithms;
        }

        public void setAlgorithms(List<AlgorithmConfig> algorithms) {
            this.algorithms = algorithms;
        }
    }

    /**
     * 路由算法配置。
     */
    public static class AlgorithmConfig {

        /**
         * 算法类名。
         */
        private String algorithm;

        /**
         * 算法参数，可能为空。
         */
        private Map<String, String> params = new HashMap<>();

        /**
         * 路由键。
         */
        private String routeKey;


        public String getAlgorithm() {
            return algorithm;
        }

        public void setAlgorithm(String algorithm) {
            this.algorithm = algorithm;
        }

        public Map<String, String> getParams() {
            return params;
        }

        public void setParams(Map<String, String> params) {
            this.params = params;
        }

        public String getRouteKey() {
            return routeKey;
        }

        public void setRouteKey(String routeKey) {
            this.routeKey = routeKey;
        }
    }

    /**
     * 数据节点配置
     */
    public static class DataNodeConfig {

        /**
         * mysql组配置
         */
        private String mysqlGroup;

        /**
         * mysql数据库配置，此配置支持缩写模式，比如db1-db20,db30这种。
         */
        private List<String> dbConfig = new ArrayList<>();

        /**
         * mysql数据库，存放展开后的信息。
         */
        private Set<String> databases = new LinkedHashSet<>();

        public String getMysqlGroup() {
            return mysqlGroup;
        }

        public void setMysqlGroup(String mysqlGroup) {
            this.mysqlGroup = mysqlGroup;
        }

        public Set<String> getDatabases() {
            return databases;
        }

        public void setDatabases(Set<String> databases) {
            this.databases = databases;
        }

        public List<String> getDbConfig() {
            return dbConfig;
        }

        public void setDbConfig(List<String> dbConfig) {
            this.dbConfig = dbConfig;
        }
    }

}
