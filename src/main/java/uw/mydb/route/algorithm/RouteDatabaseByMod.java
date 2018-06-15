package uw.mydb.route.algorithm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.conf.MydbConfig;
import uw.mydb.route.RouteAlgorithm;

/**
 * 根据给定的long值，按照库数量直接mod分库。
 * 要求value值必须为long类型。
 *
 * @author axeon
 */
public class RouteDatabaseByMod extends RouteAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(RouteDatabaseByMod.class);


    /**
     * 参数配置。
     * 子类通过继承实现配置化。
     */
    @Override
    public void config() {

    }

    @Override
    public RouteInfo calculate(MydbConfig.TableConfig tableConfig, RouteInfo routeInfo, String value) {
        long longValue = -1L;

        try {
            longValue = Long.parseLong(value);
        } catch (Exception e) {
            logger.warn("指定的value:[{}]无法格式化为long!!!");
        }

        if (longValue > -1) {
            DataNode node = dataNodes.get((int) (longValue % dataNodes.size()));
            routeInfo.setMysqlGroup(node.getMysqlGroup());
            routeInfo.setDatabase(node.getDatabase());
        }
        return routeInfo;
    }


}
