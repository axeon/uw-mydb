package uw.mydb.route.algorithm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.conf.MydbConfig;
import uw.mydb.route.RouteAlgorithm;

import java.util.ArrayList;
import java.util.List;

/**
 * 根据给定的long值，按照表数量直接mod分表。
 * 参数：routeList=mysqlGroup.database.table,mysqlGroup.database.table
 *
 * @author axeon
 */
public class RouteTableByMod extends RouteAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(RouteTableByMod.class);


    /**
     * 预设的表列表。
     */
    private List<RouteInfo> routeInfos = new ArrayList<>();

    @Override
    public void config() {
        String routeList = this.algorithmConfig.getParams().get("routeList");
        for (String route : routeList.split(",")) {
            String[] data = route.split("\\.");
            if (data.length != 3) {
                logger.error("参数配置错误！route:[{}]", route);
                continue;
            }
            routeInfos.add(new RouteInfo(data[0], data[1], data[2]));

        }
    }

    @Override
    public RouteInfo calculate(MydbConfig.TableConfig tableConfig, RouteInfo routeInfo, String value) {
        long longValue = -1L;

        try {
            longValue = Long.parseLong(value);
        } catch (Exception e) {
            logger.warn("指定的value:[{}]无法格式化为long!!!");
        }

        if (longValue == -1) {
            routeInfo = routeInfos.get(0).copy();
        } else {
            routeInfo = routeInfos.get((int) (longValue % routeInfos.size())).copy();
        }
        return routeInfo;
    }


    @Override
    public List<RouteInfo> getAllRouteList(MydbConfig.TableConfig tableConfig, List<RouteInfo> routeInfos) {
        return this.routeInfos;
    }


}