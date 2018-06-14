package uw.mydb.route.algorithm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.conf.MydbConfig;
import uw.mydb.route.RouteAlgorithm;
import uw.mydb.util.ConsistentHash;

import java.util.ArrayList;
import java.util.List;

/**
 * 基于hash的分表算法。
 * 参数：routeList=mysqlGroup.database.table,mysqlGroup.database.table
 *
 * @author axeon
 */
public class RouteTableByHash extends RouteAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(RouteTableByHash.class);

    /**
     * 一致性hash对象。
     */
    private ConsistentHash<RouteInfo> consistentHash = null;

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
        consistentHash = new ConsistentHash<>(128, routeInfos);
    }

    @Override
    public RouteInfo calculate(MydbConfig.TableConfig tableConfig, RouteInfo routeInfo, String value) {
        return consistentHash.get(value).copy();
    }

    @Override
    public List<RouteInfo> getAllRouteList(MydbConfig.TableConfig tableConfig, List<RouteInfo> routeInfos) {
        return this.routeInfos;
    }

}
