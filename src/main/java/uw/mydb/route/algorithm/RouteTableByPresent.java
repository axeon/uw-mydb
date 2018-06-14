package uw.mydb.route.algorithm;

import uw.mydb.conf.MydbConfig;
import uw.mydb.route.RouteAlgorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 按照预定分表规则分表。
 */
public class RouteTableByPresent extends RouteAlgorithm {

    /**
     * 预设的表列表。
     */
    private Map<String, RouteInfo> routeMap = new HashMap<>();

    @Override
    public void config() {
        for (Map.Entry<String, String> kv : this.algorithmConfig.getParams().entrySet()) {
            String[] data = kv.getValue().split("\\.");
            if (data.length == 3) {
                routeMap.put(kv.getKey(), new RouteInfo(data[0], data[1], data[2]));
            }
        }
    }

    @Override
    public RouteInfo calculate(MydbConfig.TableConfig tableConfig, RouteInfo routeInfo, String value) {
        RouteInfo route = routeMap.get(value);
        if (route != null) {
            routeInfo = route.copy();
        }
        return routeInfo;
    }

    @Override
    public List<RouteInfo> getAllRouteList(MydbConfig.TableConfig tableConfig, List<RouteInfo> routeInfos) {
        return new ArrayList<>(this.routeMap.values());
    }

}
