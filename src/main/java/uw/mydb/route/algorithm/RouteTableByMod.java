package uw.mydb.route.algorithm;

import uw.mydb.conf.MydbConfig;
import uw.mydb.route.RouteAlgorithm;

import java.util.List;

/**
 * 根据给定的long值，按照表数量直接mod分表。
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

    /**
     * 对于定制表来说，根本就无法匹配，直接返回所有表。
     *
     * @param tableConfig
     * @param routeInfos 携带初始值的路由信息
     * @param startValue
     * @param endValue
     * @return
     */
    @Override
    public List<RouteInfo> calculateRange(MydbConfig.TableConfig tableConfig, List<RouteInfo> routeInfos, String startValue, String endValue) {
        return this.routeInfos;
    }

    @Override
    public List<RouteInfo> getAllRouteList(MydbConfig.TableConfig tableConfig, List<RouteInfo> routeInfos) {
        return this.routeInfos;
    }


}