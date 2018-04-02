package uw.mydb.route.database;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.route.RouteAlgorithm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 根据预设信息设置表路由，此算法一般建立放在分库算法的最后，它会覆盖之前的配置。
 * params：key=routeKey，value="mysqlGroup.database"
 *
 * @author axeon
 */
public class RouteByPreset extends RouteAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(RouteByPreset.class);

    /**
     * 强行指定的参数范围。
     */
    private Map<String, String[]> params = new HashMap<>();

    /**
     * 参数配置。
     */
    @Override
    public void config() {
        for (Map.Entry<String, String> kv : algorithmConfig.getParams().entrySet()) {
            String[] data = kv.getValue().trim().split("\\.");
            if (data.length == 2) {
                params.put(kv.getKey(), data);
            }
        }
    }

    @Override
    public RouteInfo calculate(String tableName, RouteInfo routeInfo, String value) {
        if (routeInfo == null) {
            routeInfo = RouteInfo.newDataWithTable(tableName);
        }
        String[] data = params.get(value);
        if (data != null) {
            routeInfo.setMysqlGroup(data[0]);
            routeInfo.setDatabase(data[0]);
        }
        return routeInfo;
    }

    /**
     * 获得全部路由。
     *
     * @param tableName
     * @param routeInfos
     * @return
     */
    @Override
    public List<RouteInfo> getAllRouteList(String tableName, List<RouteInfo> routeInfos) {
        for (Map.Entry<String, String[]> kv : params.entrySet()) {
            String[] data = kv.getValue();
            RouteInfo routeInfo = new RouteInfo(data[0], data[1], tableName);
            if (!routeInfos.contains(routeInfo)) {
                routeInfos.add(routeInfo);
            }
        }
        return super.getAllRouteList(tableName, routeInfos);
    }
}
