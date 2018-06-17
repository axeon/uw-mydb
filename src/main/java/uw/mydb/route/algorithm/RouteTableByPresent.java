package uw.mydb.route.algorithm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.conf.MydbConfig;
import uw.mydb.route.RouteAlgorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 按照预定分表规则分表。
 * 参数：key=mysqlGroup.database.table
 */
public class RouteTableByPresent extends RouteAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(RouteTableByPresent.class);

    /**
     * 预设的表列表。
     */
    private Map<String, RouteInfo> routeMap = new HashMap<>();

    @Override
    public void config() {
        for (Map.Entry<String, String> kv : this.algorithmConfig.getParams().entrySet()) {
            String[] data = kv.getValue().split("\\.");
            if (data.length != 3) {
                logger.error("参数配置错误！key:[{}], value:[{}]", kv.getKey(), kv.getValue());
                continue;
            }
            routeMap.put(kv.getKey(), new RouteInfo(data[0], data[1], data[2]));

        }
    }

    @Override
    public RouteInfo calculate(MydbConfig.TableConfig tableConfig, RouteInfo routeInfo, String value) throws RouteException {
        RouteInfo route = routeMap.get(value);
        if (route == null) {
            logger.error("value:[{}]无法找到匹配的路由信息！");
            throw new RouteException("指定的value无法匹配路由数据!");
        }
        routeInfo = route.copy();
        return routeInfo;
    }

    @Override
    public List<RouteInfo> getAllRouteList(MydbConfig.TableConfig tableConfig, List<RouteInfo> routeInfos) throws RouteException {
        return new ArrayList<>(this.routeMap.values());
    }

}
