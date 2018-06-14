package uw.mydb.route.algorithm;

import uw.mydb.conf.MydbConfig;
import uw.mydb.route.RouteAlgorithm;
import uw.mydb.route.SchemaCheckService;

/**
 * 根据给定的key，来判断是否存在表，如果没有表，则动态自动创建以key为后缀的表。。
 * 需要在配置参数中配置mysqlGroup和database属性。
 *
 * @author axeon
 */
public class RouteTableByAutoKey extends RouteAlgorithm {

    /**
     * mysql组。
     */
    private String mysqlGroup;

    /**
     * 默认数据库。
     */
    private String database;

    @Override
    public void config() {
        this.mysqlGroup = this.algorithmConfig.getParams().get("mysqlGroup");
        this.database = this.algorithmConfig.getParams().get("database");
    }

    @Override
    public RouteInfo calculate(MydbConfig.TableConfig tableConfig, RouteInfo routeInfo, String value) {
        if (!routeInfo.checkValid()) {
            routeInfo.setMysqlGroup(mysqlGroup);
            routeInfo.setDatabase(database);
        }
        String table = new StringBuilder(routeInfo.getTable()).append('_').append(value).toString();
        routeInfo.setTable(table);
        SchemaCheckService.checkAndCreateTable(tableConfig, routeInfo);
        return routeInfo;
    }

}