package uw.mydb.route;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.http.conn.routing.RouteInfo;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.LoggerFactory;
import uw.mydb.conf.MydbConfig;
import uw.mydb.conf.MydbConfigManager;
import uw.mydb.mysql.tool.ExeSqlTask;
import uw.mydb.mysql.tool.LocalCmdCallback;
import uw.mydb.mysql.tool.SingleListTask;
import uw.mydb.mysql.tool.StringArrayListTask;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * 通过这个服务，自动检查schema，在数据库中建立对应的库表结构。
 * 当前仅支持建立，不支持后续修改的自动匹配。
 *
 * @author axeon
 */
public class SchemaCheckService {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SchemaCheckService.class);

    /**
     * 存储当前数据库中已经建立好的库表结构。
     * 结构如下：<mysqlGroup,<database,<table>>>;
     * 所有要执行的创建指令，必须经过此结构过滤，避免重复执行sql。
     */
    private static Map<String, Map<String, Set<String>>> schemaMap = new HashMap<>();

    /**
     * 调度任务。
     */
    private static ScheduledExecutorService scheduledExecutorService = null;

    /**
     * 运行状态。
     */
    private static AtomicBoolean isRunning = new AtomicBoolean(false);

    /**
     * 配置。
     */
    private static MydbConfig config = null;

    /**
     * 开启服务。
     */
    public static void start() {
        if (isRunning.compareAndSet(false, true)) {
            config = MydbConfigManager.getConfig();
            scheduledExecutorService = new ScheduledThreadPoolExecutor(1, new ThreadFactoryBuilder().setNameFormat("SchemaCheckService-%d").setDaemon(true).build(), new ThreadPoolExecutor.DiscardPolicy());
            scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    loadSchemaScript();
                }
            }, 30, TimeUnit.SECONDS);
            scheduledExecutorService.schedule(new Runnable() {
                @Override
                public void run() {
                    loadSchemaInfo();
                }
            }, 60, TimeUnit.SECONDS);
            scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    autoCreateTable();
                }
            }, 90, 3600, TimeUnit.SECONDS);
        }
    }

    /**
     * 关闭服务。
     */
    public static void stop() {
        if (isRunning.compareAndSet(true, false)) {
            scheduledExecutorService.shutdown();
        }
    }

    /**
     * 从基础节点获取表创建信息。
     */
    public static void loadSchemaScript() {
        Map<String, MydbConfig.SchemaConfig> schemaConfigMap = config.getSchemas();
        try {

            for (MydbConfig.SchemaConfig schemaConfig : schemaConfigMap.values()) {
                //获得库创建信息。
                if (!Strings.isEmpty(schemaConfig.getBaseNode())) {
                    logger.debug("loadSchemaScript[{}]库创建信息...", schemaConfig.getName());
                    new StringArrayListTask(schemaConfig.getBaseNode(), new LocalCmdCallback<ArrayList<String[]>>() {
                        @Override
                        public void onSuccess(ArrayList<String[]> strings) {
                            if (strings.size() > 0) {
                                schemaConfig.setCreateSql(strings.get(0)[1]);
                                logger.debug("loadSchemaScript[{}]库创建信息成功!", schemaConfig.getName());
                            }
                        }

                        @Override
                        public void onFail(int errorNo, String message) {
                            logger.error("loadSchemaScript[{}]库创建信息报错: {}", schemaConfig.getName(), message);

                        }
                    }).setSql("SHOW CREATE DATABASE " + schemaConfig.getName()).run();
                    //获得表创建信息。
                    for (MydbConfig.TableConfig tableConfig : schemaConfig.getTables().values()) {
                        logger.debug("loadSchemaScript[{}.{}.{}]表创建信息...", schemaConfig.getBaseNode(), schemaConfig.getName(), tableConfig.getName());

                        if (Strings.isEmpty(tableConfig.getCreateSql())) {
                            new StringArrayListTask(schemaConfig.getBaseNode(), new LocalCmdCallback<ArrayList<String[]>>() {
                                @Override
                                public void onSuccess(ArrayList<String[]> strings) {
                                    if (strings.size() > 0) {
                                        tableConfig.setCreateSql(strings.get(0)[1]);
                                        logger.debug("loadSchemaScript[{}.{}.{}]表创建信息成功!", schemaConfig.getBaseNode(), schemaConfig.getName(), tableConfig.getName());
                                    }
                                }

                                @Override
                                public void onFail(int errorNo, String message) {
                                    logger.error("loadSchemaScript[{}.{}.{}]表创建信息报错: {}", schemaConfig.getBaseNode(), schemaConfig.getName(), tableConfig.getName(), message);
                                }
                            }).setSql("SHOW CREATE TABLE " + schemaConfig.getName() + "." + tableConfig.getName()).run();
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

    }


    /**
     * 载入所有的数据库表信息。
     */
    public static void loadSchemaInfo() {
        Map<String, MydbConfig.MysqlGroupConfig> groupConfigMap = config.getMysqlGroups();
        try {
            for (String groupName : groupConfigMap.keySet()) {
                //测试下内部指令。
                new SingleListTask(groupName, new LocalCmdCallback<ArrayList<String>>() {
                    @Override
                    public void onSuccess(ArrayList<String> strings) {
                        for (String database : strings) {
                            //过滤系统数据库。
                            if (database.equals("mysql") || database.equals("sys") || database.equals("information_schema") || database.equals("performance_schema") || database.equals("test")) {
                                continue;
                            }
                            setSchemaStatus(groupName, database, null);
                            logger.debug("正在加载数据库[{}.{}]信息...", groupName, database);
                            new SingleListTask(groupName, new LocalCmdCallback<ArrayList<String>>() {
                                @Override
                                public void onSuccess(ArrayList<String> strings) {
                                    for (String table : strings) {
                                        setSchemaStatus(groupName, database, table);
//                                        logger.trace("正在加载数据表[{}.{}.{}]信息...", groupName, database, table);
                                    }
                                }

                                @Override
                                public void onFail(int errorNo, String message) {
                                    logger.error("加载数据库[{}.{}]报错：{}...", groupName, database, message);

                                }
                            }).setSql("show tables from " + database).run();
                        }
                    }

                    @Override
                    public void onFail(int errorNo, String message) {
                        logger.error("加载主机[{}]数据库失败!", groupName);
                    }
                }).setSql("show databases").run();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 检查并创建表。
     */
    public static void checkAndCreateTable(MydbConfig.TableConfig tableConfig, RouteAlgorithm.RouteInfo routeInfo) {
        if (!checkSchemaExists(routeInfo.getMysqlGroup(), routeInfo.getDatabase(), routeInfo.getTable())) {
            //先检查库的情况。
            if (!checkSchemaExists(routeInfo.getMysqlGroup(), routeInfo.getDatabase(), null)) {
                logger.info("开始自动建库{}.{}...", routeInfo.getMysqlGroup(), routeInfo.getDatabase());
                //开始建库
                new ExeSqlTask(routeInfo.getMysqlGroup(), new LocalCmdCallback<Long>() {
                    @Override
                    public void onSuccess(Long aLong) {
                        if (aLong != null && aLong == 1) {
                            //设置建库状态。
                            setSchemaStatus(routeInfo.getMysqlGroup(), routeInfo.getDatabase(), null);
                            logger.info("自动建库{}.{}成功！", routeInfo.getMysqlGroup(), routeInfo.getDatabase());
                        } else {
                            logger.error("自动建库{}.{}失败！", routeInfo.getMysqlGroup(), routeInfo.getDatabase());
                        }
                    }

                    @Override
                    public void onFail(int errorNo, String message) {
                        logger.error("自动建库{}.{}失败！原因：{}", routeInfo.getMysqlGroup(), routeInfo.getDatabase(), message);
                    }
                }).setSql("create database " + routeInfo.getDatabase()).run();
            }

            logger.info("开始自动建表{}.{}.{}...", routeInfo.getMysqlGroup(), routeInfo.getDatabase(), routeInfo.getTable());
            //盘整参数。
            String sql = tableConfig.getCreateSql();
            if (sql == null) {
                logger.error("建表{}.{}.{}失败，未找到SQL信息...", routeInfo.getMysqlGroup(), routeInfo.getDatabase(), routeInfo.getTable());
                return;
            }
            sql = sql.replaceFirst(tableConfig.getName(), routeInfo.getDatabase() + "`.`" + routeInfo.getTable());
            //开始建表。
            new ExeSqlTask(routeInfo.getMysqlGroup(), new LocalCmdCallback<Long>() {
                @Override
                public void onSuccess(Long aLong) {
                    //设置建库状态。
                    setSchemaStatus(routeInfo.getMysqlGroup(), routeInfo.getDatabase(), routeInfo.getTable());
                    logger.info("自动建表{}.{}.{}成功！", routeInfo.getMysqlGroup(), routeInfo.getDatabase(), routeInfo.getTable());
                }

                @Override
                public void onFail(int errorNo, String message) {
                    logger.error("自动建表{}.{}.{}失败！原因：{}", routeInfo.getMysqlGroup(), routeInfo.getDatabase(), routeInfo.getTable(), message);
                }
            }).setSql(sql).run();
        }
    }

    /**
     * 根据配置文件，自动生成库表结构。
     */
    public static void autoCreateTable() {
        Map<String, MydbConfig.SchemaConfig> schemaConfigMap = config.getSchemas();
        for (MydbConfig.SchemaConfig schemaConfig : schemaConfigMap.values()) {
            for (MydbConfig.TableConfig tableConfig : schemaConfig.getTables().values()) {
                //检查算法情况。
                List<RouteAlgorithm.RouteInfo> routeInfos = null;
                try {
                    routeInfos = RouteManager.getAllRouteList(tableConfig);
                    for (RouteAlgorithm.RouteInfo routeInfo : routeInfos) {
                        checkAndCreateTable(tableConfig, routeInfo);
                    }
                } catch (RouteAlgorithm.RouteException e) {
                    logger.error("自动创建表错误：" + e.getMessage(), e);
                }

            }
        }
    }


    /**
     * 检查指定的库表是否建立
     *
     * @param mysqlGroup
     * @param database
     * @param table      当设置为null时，不检查表
     * @return
     */
    public static boolean checkSchemaExists(String mysqlGroup, String database, String table) {
        //group == null，直接返回true吧
        if (mysqlGroup == null) {
            return true;
        }
        Map<String, Set<String>> dbMap = schemaMap.get(mysqlGroup);
        if (dbMap == null) {
            return false;
        }
        //database == null，说明不检测database，直接返回true吧
        if (database == null) {
            return true;
        }
        Set<String> tables = dbMap.get(database);
        if (tables == null) {
            return false;
        }
        //table == null，table，直接返回true吧
        if (table == null) {
            return true;
        }
        if (!tables.contains(table)) {
            return false;
        }
        return true;
    }

    /**
     * 检查指定的库表是否建立
     *
     * @param mysqlGroup
     * @param database
     * @param tableMatch 表名前缀
     * @return
     */
    public static List<String> getTableList(String mysqlGroup, String database, String tableMatch) {
        List<String> list = new ArrayList<>();

        //group == null，直接返回
        if (mysqlGroup == null) {
            return list;
        }
        Map<String, Set<String>> dbMap = schemaMap.get(mysqlGroup);
        if (dbMap == null) {
            return list;
        }
        //database == null，说明不检测database，直接返回true吧
        if (database == null) {
            return list;
        }
        Set<String> tables = dbMap.get(database);
        list = tables.stream().filter(x -> x.matches(tableMatch)).collect(Collectors.toList());
        return list;
    }

    /**
     * 设置schema状态。
     *
     * @param mysqlGroup
     * @param database
     * @param table
     */
    public static void setSchemaStatus(String mysqlGroup, String database, String table) {
        if (mysqlGroup != null) {
            Map<String, Set<String>> dbMap = schemaMap.get(mysqlGroup);
            if (dbMap == null) {
                dbMap = new HashMap<>();
                schemaMap.put(mysqlGroup, dbMap);
            }
            if (database != null) {
                Set<String> tables = dbMap.get(database);
                if (tables == null) {
                    tables = new HashSet<>();
                    dbMap.put(database, tables);
                }
                if (table != null) {
                    tables.add(table);
                }
            }
        }
    }
}
