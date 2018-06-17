package uw.mydb.route.algorithm;

import uw.mydb.conf.MydbConfig;
import uw.mydb.route.RouteAlgorithm;
import uw.mydb.route.SchemaCheckService;

/**
 * 直接将指定表重定向到指定datanode上去。
 * 需要在配置参数中配置mysqlGroup和database属性。
 *
 * @author axeon
 */
public class RouteTableByDataNode extends RouteAlgorithm {

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
    public RouteInfo calculate(MydbConfig.TableConfig tableConfig, RouteInfo routeInfo, String value) throws RouteException {
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