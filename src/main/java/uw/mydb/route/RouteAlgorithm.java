package uw.mydb.route;

import uw.mydb.conf.MydbConfig;

import java.security.PrivilegedActionException;
import java.util.*;

/**
 * 动态分表算法，一般来说表是完全动态创建的。
 *
 * @author axeon
 */
public abstract class RouteAlgorithm {

    /**
     * 路由配置。
     */
    protected String routeName;

    /**
     * 算法配置。
     */
    protected MydbConfig.AlgorithmConfig algorithmConfig;

    /**
     * 数据节点配置。
     */
    protected List<DataNode> dataNodes = new ArrayList<>();

    /**
     * 根据分库分表参数初始化。
     *
     * @param algorithmConfig 算法配置
     * @param dataNodeConfigs 数据节点配置
     */
    public void init(String routeName, MydbConfig.AlgorithmConfig algorithmConfig, List<MydbConfig.DataNodeConfig> dataNodeConfigs) {
        this.routeName = routeName;
        this.algorithmConfig = algorithmConfig;
        //整理dataNode列表
        for (MydbConfig.DataNodeConfig dataNodeConfig : dataNodeConfigs) {
            for (String database : dataNodeConfig.getDatabases()) {
                dataNodes.add(new DataNode(dataNodeConfig.getMysqlGroup(), database));
            }
        }
    }

    /**
     * 获得算法配置。
     *
     * @return
     */
    public MydbConfig.AlgorithmConfig getAlgorithmConfig() {
        return algorithmConfig;
    }

    /**
     * 参数配置。
     * 子类通过继承实现配置化。
     */
    public abstract void config();


    /**
     * 根据给定的值，计算出归属表名。
     * 此方法一般增，删，改用。
     *
     * @param tableConfig
     * @param routeInfo   携带初始值的路由信息
     * @param value       分表数值
     * @return 修正后的路由信息
     */
    public RouteInfo calculate(MydbConfig.TableConfig tableConfig, RouteInfo routeInfo, String value) throws RouteException {
        throw new UnsupportedOperationException();
    }

    /**
     * 根据给定的值，计算出归属表名。
     * 此方法一般查询用。
     *
     * @param tableConfig
     * @param routeInfos  携带初始值的路由信息
     * @return 修正后的路由信息
     */
    public Map<String, RouteInfo> calculate(MydbConfig.TableConfig tableConfig, Map<String, RouteInfo> routeInfos, List<String> values) throws RouteException {
        for (String value : values) {
            RouteInfo routeInfo = routeInfos.get(value);
            if (routeInfo == null) {
                routeInfo = RouteInfo.newDataWithTable(tableConfig.getName());
                routeInfos.put(value, routeInfo);
            }
            calculate(tableConfig, routeInfo, value);
        }
        return routeInfos;
    }

    /**
     * 根据给定之后，计算出所有表名。
     * 此方法一般查询用。
     *
     * @param tableConfig
     * @param routeInfos  携带初始值的路由信息
     * @param startValue
     * @param endValue
     * @return 表名列表
     */
    public List<RouteInfo> calculateRange(MydbConfig.TableConfig tableConfig, List<RouteInfo> routeInfos, String startValue, String endValue) throws RouteException {
        throw new RouteException("不支持范围计算!");
    }


    /**
     * 获得预设的表名信息。
     * 返回集合中String。
     * 默认使用分库设置，加载一次。
     *
     * @return
     */
    public RouteInfo getDefaultRoute(MydbConfig.TableConfig tableConfig, RouteInfo routeInfo) throws RouteException {
        return routeInfo;
    }

    /**
     * 获得预设的表名信息。
     * 返回集合中String。
     * 默认使用分库设置，加载一次。
     *
     * @return
     */
    public List<RouteInfo> getAllRouteList(MydbConfig.TableConfig tableConfig, List<RouteInfo> routeInfos) throws RouteException {
        if (routeInfos == null || routeInfos.size() == 0) {
            for (DataNode dataNode : dataNodes) {
                RouteInfo routeInfo = RouteInfo.newDataWithTable(tableConfig.getName());
                routeInfo.setDataNode(dataNode);
                routeInfos.add(routeInfo);
            }
        }
        return routeInfos;
    }

    /**
     * 此方法用于返回创建表信息。
     *
     * @param tableConfig
     * @param routeInfos
     * @return
     */
    public List<RouteInfo> getRouteListForCreate(MydbConfig.TableConfig tableConfig, List<RouteInfo> routeInfos) throws RouteException {
        return getAllRouteList(tableConfig, routeInfos);
    }

    /**
     * 存放路由Key。
     */
    public static class RouteKeyData {

        /**
         * key，用于优化内存占用。
         */
        private String key;

        /**
         * value，用于优化内存占用。
         */
        private RouteKeyValue value;

        /**
         * 参数对。
         */
        private Map<String, RouteKeyValue> params;


        /**
         * 检查是否有key。
         *
         * @return
         */
        public boolean checkKeyExists() {
            if (key != null || params != null) {
                return true;
            } else {
                return false;
            }
        }

        /**
         * 返回key列表。
         *
         * @return
         */
        public String keyString() {
            if (key != null) {
                return key;
            } else {
                StringBuilder sb = new StringBuilder();
                for (String key : params.keySet()) {
                    sb.append(key).append(",");
                }
                if (sb.charAt(sb.length() - 1) == ',') {
                    sb.deleteCharAt(sb.length() - 1);
                }
                return sb.toString();
            }
        }

        /**
         * 有空数值，返回true。
         *
         * @return
         */
        public boolean isEmptyValue() {
            if (value != null) {
                return value.isEmpty();
            }
            if (params != null) {
                for (RouteKeyValue rkv : params.values()) {
                    if (rkv.isEmpty()) {
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * 是否为单一数值。
         *
         * @return
         */
        public boolean isSingle() {
            return params == null;
        }

        /**
         * 获得单一值列表。
         *
         * @return
         */
        public RouteKeyValue getValue() {
            return value;
        }

        /**
         * 获得数值列表
         *
         * @return
         */
        public Collection<RouteKeyValue> getValues() {
            if (params != null) {
                return params.values();
            } else {
                return null;
            }
        }

        /**
         * 初始化key
         *
         * @param key
         */
        public void initKey(String key) {
            if (this.key == null) {
                this.key = key;
                this.value = new RouteKeyValue();
            } else {
                if (!this.key.equals(key)) {
                    if (params == null) {
                        params = new HashMap<>();
                        params.put(this.key, this.value);
                        this.key = null;
                        this.value = null;
                    }
                    params.put(key, new RouteKeyValue());
                }
            }
        }

        /**
         * 获得数值。
         *
         * @param key
         * @return
         */
        public RouteKeyValue getValue(String key) {
            if (params != null) {
                return params.get(key);
            }

            if (this.key == null) {
                return null;
            }

            if (this.key.equals(key)) {
                return value;
            } else {
                return null;
            }

        }
    }

    /**
     * 用于存储路由算法异常。
     */
    public static class RouteException extends Exception {
        /**
         * Constructs a new exception with {@code null} as its detail message.
         * The cause is not initialized, and may subsequently be initialized by a
         * call to {@link #initCause}.
         */
        public RouteException() {
        }

        /**
         * Constructs a new exception with the specified detail message.  The
         * cause is not initialized, and may subsequently be initialized by
         * a call to {@link #initCause}.
         *
         * @param message the detail message. The detail message is saved for
         *                later retrieval by the {@link #getMessage()} method.
         */
        public RouteException(String message) {
            super(message);
        }

        /**
         * Constructs a new exception with the specified detail message and
         * cause.  <p>Note that the detail message associated with
         * {@code cause} is <i>not</i> automatically incorporated in
         * this exception's detail message.
         *
         * @param message the detail message (which is saved for later retrieval
         *                by the {@link #getMessage()} method).
         * @param cause   the cause (which is saved for later retrieval by the
         *                {@link #getCause()} method).  (A <tt>null</tt> value is
         *                permitted, and indicates that the cause is nonexistent or
         *                unknown.)
         * @since 1.4
         */
        public RouteException(String message, Throwable cause) {
            super(message, cause);
        }

        /**
         * Constructs a new exception with the specified cause and a detail
         * message of <tt>(cause==null ? null : cause.toString())</tt> (which
         * typically contains the class and detail message of <tt>cause</tt>).
         * This constructor is useful for exceptions that are little more than
         * wrappers for other throwables (for example, {@link
         * PrivilegedActionException}).
         *
         * @param cause the cause (which is saved for later retrieval by the
         *              {@link #getCause()} method).  (A <tt>null</tt> value is
         *              permitted, and indicates that the cause is nonexistent or
         *              unknown.)
         * @since 1.4
         */
        public RouteException(Throwable cause) {
            super(cause);
        }

        /**
         * Constructs a new exception with the specified detail message,
         * cause, suppression enabled or disabled, and writable stack
         * trace enabled or disabled.
         *
         * @param message            the detail message.
         * @param cause              the cause.  (A {@code null} value is permitted,
         *                           and indicates that the cause is nonexistent or unknown.)
         * @param enableSuppression  whether or not suppression is enabled
         *                           or disabled
         * @param writableStackTrace whether or not the stack trace should
         *                           be writable
         * @since 1.7
         */
        public RouteException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }

    /**
     * 路由数值类型。
     */
    public static class RouteKeyValue {

        /**
         * 空值
         */
        public static final int NULL = 0;

        /**
         * 单个数值
         */
        public static final int SINGLE = 1;

        /**
         * RANGE类型
         */
        public static final int RANGE = 2;

        /**
         * 多值类型
         */
        public static final int MULTI = 3;

        /**
         * 类型
         */
        private int type = NULL;

        /**
         * 数值1
         */
        private String value1;

        /**
         * 数值2
         */
        private String value2;

        /**
         * 多值类型
         */
        private List<String> values;

        /**
         * 是否为空。
         *
         * @return
         */
        public boolean isEmpty() {
            switch (type) {
                case SINGLE:
                    return value1 == null;
                case RANGE:
                    return value1 == null && value2 == null;
                case MULTI:
                    return values == null;
                default:
                    return true;
            }
        }

        /**
         * 针对范围运行有时候只有一个值来进行优化。
         */
        public void calcType() {
            if (value1 == null || value2 == null) {
                type = SINGLE;
                if (value1 == null && value2 != null) {
                    value1 = value2;
                }
                if (value1 != null && value2 == null) {
                    value2 = value1;
                }
            }
        }


        public void putValue(String value) {
            type = SINGLE;
            this.value1 = value;
        }

        public void putRangeStart(String value1) {
            type = RANGE;
            this.value1 = value1;
        }

        public void putRangeEnd(String value2) {
            type = RANGE;
            this.value2 = value2;
        }

        public void putValues(List<String> values) {
            type = MULTI;
            this.values = values;
        }

        public String getValue1() {
            return value1;
        }

        public String getValue2() {
            return value2;
        }

        public List<String> getValues() {
            return values;
        }

        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }
    }

    /**
     * 存放展开后的DataNode。
     */
    protected static class DataNode {

        /**
         * mysql配置组名
         */
        String mysqlGroup;

        /**
         * 对应的数据库名。
         */
        String database;

        public DataNode(String mysqlGroup, String database) {
            this.mysqlGroup = mysqlGroup;
            this.database = database;
        }

        public String getMysqlGroup() {
            return mysqlGroup;
        }

        public void setMysqlGroup(String mysqlGroup) {
            this.mysqlGroup = mysqlGroup;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        @Override
        public String toString() {
            return new StringBuilder().append(this.mysqlGroup).append('.').append(database).toString();
        }
    }

    /**
     * 用于优化存储RouteInfo
     */
    public static class RouteInfoData {

        private RouteInfo routeInfo;

        private Set<RouteInfo> routeInfos;

        /**
         * 设置一个RouteInfo Set.
         *
         * @param routeInfos
         */
        public void setAll(Set<RouteInfo> routeInfos) {
            if (routeInfos != null && routeInfos.size() == 1) {
                this.routeInfo = routeInfos.iterator().next();
                this.routeInfos = null;
            } else {
                this.routeInfo = null;
                this.routeInfos = routeInfos;
            }
        }

        /**
         * 返回是否单条数据。
         *
         * @return
         */
        public boolean isSingle() {
            return routeInfos == null;
        }

        public void setSingle(RouteInfo routeInfo) {
            this.routeInfo = routeInfo;
            this.routeInfos = null;
        }

        public RouteInfo getRouteInfo() {
            return routeInfo;
        }

        public Set<RouteInfo> getRouteInfos() {
            return routeInfos;
        }
    }

    /**
     * 路由结果。
     */
    public static class RouteInfo {

        /**
         * 对应的mysqlGroup
         */
        private String mysqlGroup;

        /**
         * 分组对应数据。
         */
        private String database;

        /**
         * 分组对应表名。
         */
        private String table;

        public RouteInfo(String mysqlGroup, String database, String table) {
            this.mysqlGroup = mysqlGroup;
            this.database = database;
            this.table = table;
        }

        /**
         * new一个全都是空的对象。
         *
         * @return
         */
        public static RouteInfo newEmptyData() {
            return new RouteInfo(null, null, null);
        }

        /**
         * 只有表名的新实例。
         *
         * @param table
         * @return
         */
        public static RouteInfo newDataWithTable(String table) {
            return new RouteInfo(null, null, table);
        }

        /**
         * new一个只有单个RouteInfo的Map。
         *
         * @param routeInfo
         * @return
         */
        public static Map<String, RouteInfo> newMapWithRouteInfo(RouteInfo routeInfo) {
            HashMap<String, RouteInfo> map = new HashMap<>();
            map.put("", routeInfo);
            return map;
        }

        /**
         * new一个只有单个RouteInfo的List。
         *
         * @param routeInfo
         * @return
         */
        public static List<RouteInfo> newListWithRouteInfo(RouteInfo routeInfo) {
            List<RouteInfo> list = new ArrayList<>();
            list.add(routeInfo);
            return list;
        }

        /**
         * 检查是否合法。
         * 必须都非null值才合法。
         *
         * @return
         */
        public boolean checkValid() {
            return mysqlGroup != null && database != null && table != null;
        }

        /**
         * 比较两个字符串是否相等。
         *
         * @param str1
         * @param str2
         * @return
         */
        private boolean equals(String str1, String str2) {
            if (str1 == null) {
                return str2 == null;
            }
            return str1.equals(str2);
        }

        /**
         * 复制一个RouteInfo。
         */
        public RouteInfo copy() {
            return new RouteInfo(mysqlGroup, database, table);
        }

        /**
         * 覆盖hashCode。
         *
         * @return
         */
        @Override
        public int hashCode() {
            int hashcode = 31;
            hashcode += (mysqlGroup == null ? 0 : mysqlGroup.hashCode());
            hashcode += (database == null ? 0 : database.hashCode());
            hashcode += (table == null ? 0 : table.hashCode());
            return hashcode;
        }

        /**
         * 覆盖equals方法。
         *
         * @param obj
         * @return
         */
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof RouteInfo) {
                RouteInfo entity = (RouteInfo) obj;
                boolean isEquals = equals(mysqlGroup, entity.mysqlGroup);
                isEquals = isEquals && equals(database, entity.database);
                isEquals = isEquals && equals(table, entity.table);
                return isEquals;
            }
            return false;
        }

        @Override
        public String toString() {
            return new StringBuilder().append(mysqlGroup).append('.').append(database).append('.').append(table).toString();
        }

        /**
         * 设置DataNode的数值。
         *
         * @param dataNode
         */
        public void setDataNode(DataNode dataNode) {
            this.mysqlGroup = dataNode.getMysqlGroup();
            this.database = dataNode.getDatabase();
        }

        public String getMysqlGroup() {
            return mysqlGroup;
        }

        public void setMysqlGroup(String mysqlGroup) {
            this.mysqlGroup = mysqlGroup;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }
    }

}
