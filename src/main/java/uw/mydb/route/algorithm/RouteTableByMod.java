package uw.mydb.route.algorithm;

import uw.mydb.route.RouteAlgorithm;

import java.util.List;

/**
 * 根据给定的long值，按照表数量直接mod分表。
 * 数据库表一般都是预建立的。
 * 一般含有1个参数：
 * 1.config: 配置库表信息,格式为mysqlGroup.database.table逗号分隔列表，此数值会覆盖datanode配置。
 *
 * @author axeon
 */
public class RouteTableByMod extends RouteAlgorithm {

    /**
     * 预设的表列表。
     */
    private List<RouteInfo> routeInfos;

    @Override
    public void config() {

    }

    @Override
    public RouteInfo calculate(String tableName, RouteInfo routeInfo, String value) {
        long longValue = -1L;

        try {
            longValue = Long.parseLong(value);
        } catch (Exception e) {
        }

        if (longValue == -1) {
            routeInfo = routeInfos.get(0);
        } else {
            routeInfo = routeInfos.get((int) (longValue % routeInfos.size()));
        }
        return routeInfo;
    }

    /**
     * 对于定制表来说，根本就无法匹配，直接返回所有表。
     *
     * @param tableName
     * @param routeInfos 携带初始值的路由信息
     * @param startValue
     * @param endValue
     * @return
     */
    @Override
    public List<RouteInfo> calculateRange(String tableName, List<RouteInfo> routeInfos, String startValue, String endValue) {
        return this.routeInfos;
    }

    @Override
    public List<RouteInfo> getAllRouteList(String tableName, List<RouteInfo> routeInfos) {
        return this.routeInfos;
    }


}