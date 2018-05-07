package uw.mydb.route.algorithm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.route.RouteAlgorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 根据给定的代码，给出指定表名。
 * 数据库表一般都是预建立的。
 * 一般含有两个参数：
 * 1.config: 配置库表信息,格式为mysqlGroup.database.table逗号分隔列表，此数值会覆盖datanode配置。
 * 2.pattern: 数值整形，格式为left:len/mid:pos,len/right:len这种，直接重整为表后缀。不配置则为整体匹配。
 *
 * @author axeon
 */
public class RouteTableByCode extends RouteAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(RouteTableByCode.class);


    /**
     * 预设的表列表。
     */
    private Map<String, RouteInfo> routeInfoMap = new HashMap<>();

    /**
     * 匹配模式。
     */
    private String patternType;

    /**
     * 截取开始位置。
     */
    private int startPos = -1;

    /**
     * 截取长度。
     */
    private int length = -1;

    @Override
    public void config() {
        String config = algorithmConfig.getParams().get("config");
        String pattern = algorithmConfig.getParams().get("pattern");
        String[] configValues = config.split(",");
        //构造路由信息列表。
        for (String configValue : configValues) {
            String[] values = configValue.split("\\.");
            if (values.length == 3) {
                RouteInfo routeInfo = new RouteInfo(values[0], values[1], values[2]);
                routeInfoMap.put(routeInfo.getTable(), routeInfo);
            }
        }

        //构造匹配信息
        if (pattern.startsWith("left:")) {
            patternType = "left";
            pattern = pattern.substring(5);
            try {
                length = Integer.parseInt(pattern);
            } catch (Exception e) {
            }
        } else if (pattern.startsWith("mid:")) {
            patternType = "mid";
            pattern = pattern.substring(4);
            String[] values = pattern.split(",");
            if (values.length > 0) {
                try {
                    startPos = Integer.parseInt(values[0].trim());
                } catch (Exception e) {
                }
            }
            if (values.length == 2) {
                try {
                    length = Integer.parseInt(values[1].trim());
                } catch (Exception e) {
                }
            }
        } else if (pattern.startsWith("right:")) {
            patternType = "right";
            pattern = pattern.substring(6);
            try {
                length = Integer.parseInt(pattern);
            } catch (Exception e) {
            }
        }
    }

    @Override
    public RouteInfo calculate(String tableName, RouteInfo routeInfo, String value) {
        switch (patternType) {
            case "left":
                if (length > 0) {
                    value = value.substring(0, Math.min(value.length(), length));
                }
                break;
            case "mid":
                if (startPos > -1 && length > 0) {
                    value = value.substring(Math.min(value.length(), startPos), Math.min(value.length(), startPos + length));
                }
                break;
            case "right":
                if (length > 0) {
                    value = value.substring(value.length() - Math.min(value.length(), length));
                }
                break;
            default:
                break;

        }
        String newTable = new StringBuilder(tableName).append("_").append(value).toString();
        RouteInfo newRouteInfo = routeInfoMap.get(newTable);
        if (newRouteInfo == null) {
            return routeInfo;
        } else {
            return newRouteInfo;
        }
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
        return routeInfos;
    }

    @Override
    public List<RouteInfo> getAllRouteList(String tableName, List<RouteInfo> routeInfos) {
        return new ArrayList<>(routeInfoMap.values());
    }

}