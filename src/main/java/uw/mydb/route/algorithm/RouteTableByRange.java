package uw.mydb.route.algorithm;

import uw.mydb.conf.MydbConfig;
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
public class RouteTableByRange extends RouteAlgorithm {

    /**
     * 预设的表列表。
     */
    private List<RouteInfo> routeInfos;

    @Override
    public void config() {

    }

    @Override
    public RouteInfo calculate(MydbConfig.TableConfig tableConfig, RouteInfo routeInfo, String value) {
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


}