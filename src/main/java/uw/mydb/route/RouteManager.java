package uw.mydb.route;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.conf.MydbConfig;
import uw.mydb.conf.MydbConfigManager;

import java.util.*;

/**
 * 路由管理器。
 *
 * @author axeon
 */
public class RouteManager {

    private static final Logger logger = LoggerFactory.getLogger(RouteManager.class);

    /**
     * 算法实例的管理器
     */
    private static Map<String, List<RouteAlgorithm>> routeAlgorithmMap = new HashMap<>();

    /**
     * 配置信息。
     */
    private static MydbConfig config = MydbConfigManager.getConfig();


    /**
     * 初始化管理器，缓存算法实例。
     */
    public static void init() {
        //填充算法列表。
        for (MydbConfig.RouteConfig routeConfig : config.getRoutes().values()) {
            List<MydbConfig.DataNodeConfig> dataNodeConfigs = routeConfig.getDataNodes();
            List<MydbConfig.AlgorithmConfig> algorithmConfigs = routeConfig.getAlgorithms();
            ArrayList<RouteAlgorithm> routeAlgorithms = new ArrayList<>();
            for (MydbConfig.AlgorithmConfig algorithmConfig : algorithmConfigs) {
                try {
                    Class clazz = Class.forName(algorithmConfig.getAlgorithm());
                    Object object = clazz.newInstance();
                    if (object instanceof RouteAlgorithm) {
                        RouteAlgorithm algorithm = (RouteAlgorithm) object;
                        algorithm.init(routeConfig.getName(), algorithmConfig, dataNodeConfigs);
                        algorithm.config();
                        routeAlgorithms.add(algorithm);
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
            routeAlgorithmMap.put(routeConfig.getName(), routeAlgorithms);
        }
    }

    /**
     * 根据route名称获得算法列表。
     *
     * @return
     */
    public static List<RouteAlgorithm> getRouteAlgorithmList(String route) {
        return routeAlgorithmMap.get(route);
    }

    /**
     * 获得tableConfig配置。
     *
     * @param tablename
     * @return
     */
    public static MydbConfig.TableConfig getTableConfig(MydbConfig.SchemaConfig schema, String tablename) {
        return schema.getTables().get(tablename);
    }

    /**
     * 获得匹配列的map。
     *
     * @param tablename
     * @return
     */
    public static RouteAlgorithm.RouteKeyData getParamMap(MydbConfig.SchemaConfig schema, String tablename) {
        return getParamMap(getTableConfig(schema, tablename));
    }

    /**
     * 获得匹配列的map。
     *
     * @param tableConfig
     * @return
     */
    public static RouteAlgorithm.RouteKeyData getParamMap(MydbConfig.TableConfig tableConfig) {
        if (tableConfig == null) {
            return null;
        }
        RouteAlgorithm.RouteKeyData keyData = new RouteAlgorithm.RouteKeyData();
        MydbConfig.RouteConfig routeConfig = config.getRoutes().get(tableConfig.getRoute());
        List<MydbConfig.AlgorithmConfig> algorithmConfigs = routeConfig.getAlgorithms();
        for (MydbConfig.AlgorithmConfig algorithmConfig : algorithmConfigs) {
            keyData.initKey(algorithmConfig.getRouteKey());
        }
        return keyData;
    }

    /**
     * 获得路由信息。
     *
     * @param tableConfig
     * @param keyData
     * @return
     */
    public static RouteAlgorithm.RouteInfoData calculate(MydbConfig.TableConfig tableConfig, RouteAlgorithm.RouteKeyData keyData) {
        RouteAlgorithm.RouteInfoData routeInfoData = new RouteAlgorithm.RouteInfoData();
        //构造空路由配置。
        RouteAlgorithm.RouteInfo routeInfo = RouteAlgorithm.RouteInfo.newDataWithTable(tableConfig.getName());
        routeInfoData.setSingle(routeInfo);
        //获得路由算法列表。
        List<RouteAlgorithm> routeAlgorithms = getRouteAlgorithmList(tableConfig.getRoute());
        if (routeAlgorithms == null) {
            return routeInfoData;
        }
        for (RouteAlgorithm routeAlgorithm : routeAlgorithms) {
            RouteAlgorithm.RouteKeyValue value = keyData.getValue(routeAlgorithm.getAlgorithmConfig().getRouteKey());
            if (value.getType() == RouteAlgorithm.RouteKeyValue.SINGLE) {
                routeInfo = routeAlgorithm.calculate(tableConfig.getName(), routeInfo, value.getValue1());
                routeInfoData.setSingle(routeInfo);
            } else if (value.getType() == RouteAlgorithm.RouteKeyValue.RANGE) {
                Set<RouteAlgorithm.RouteInfo> set = new HashSet<>();
                if (routeInfoData.isSingle()) {
                    List<RouteAlgorithm.RouteInfo> list = routeAlgorithm.calculateRange(tableConfig.getName(), RouteAlgorithm.RouteInfo.newListWithRouteInfo(routeInfoData.getRouteInfo()), value.getValue1(), value.getValue2());
                    set.addAll(list);
                } else {
                    for (RouteAlgorithm.RouteInfo ri : routeInfoData.getRouteInfos()) {
                        List<RouteAlgorithm.RouteInfo> list = routeAlgorithm.calculateRange(tableConfig.getName(), RouteAlgorithm.RouteInfo.newListWithRouteInfo(ri), value.getValue1(), value.getValue2());
                        set.addAll(list);
                    }
                }
                routeInfoData.setAll(set);
            } else if (value.getType() == RouteAlgorithm.RouteKeyValue.MULTI) {
                Set<RouteAlgorithm.RouteInfo> set = new HashSet<>();
                if (routeInfoData.isSingle()) {
                    Map<String, RouteAlgorithm.RouteInfo> map = routeAlgorithm.calculate(tableConfig.getName(), RouteAlgorithm.RouteInfo.newMapWithRouteInfo(routeInfoData.getRouteInfo()), value.getValues());
                    set.addAll(map.values());
                } else {
                    for (RouteAlgorithm.RouteInfo ri : routeInfoData.getRouteInfos()) {
                        Map<String, RouteAlgorithm.RouteInfo> map = routeAlgorithm.calculate(tableConfig.getName(), RouteAlgorithm.RouteInfo.newMapWithRouteInfo(ri), value.getValues());
                        set.addAll(map.values());
                    }
                }
                routeInfoData.setAll(set);
            } else {
                //此时说明参数没有匹配上。
                routeInfo = routeAlgorithm.getDefaultRoute(tableConfig.getName(), routeInfo);
                routeInfoData.setSingle(routeInfo);
            }
        }
        return routeInfoData;
    }

    /**
     * 获得要创建表的信息。
     *
     * @param tableConfig
     * @return
     */
    public static List<RouteAlgorithm.RouteInfo> getAllRouteList(MydbConfig.TableConfig tableConfig) {
        List<RouteAlgorithm> routeAlgorithms = getRouteAlgorithmList(tableConfig.getRoute());
        List<RouteAlgorithm.RouteInfo> routeInfo = new ArrayList<>();
        for (RouteAlgorithm routeAlgorithm : routeAlgorithms) {
            routeInfo = routeAlgorithm.getAllRouteList(tableConfig.getName(), routeInfo);
        }
        return routeInfo;
    }

}
