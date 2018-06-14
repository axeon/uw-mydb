package uw.mydb.route.algorithm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.conf.MydbConfig;
import uw.mydb.route.RouteAlgorithm;
import uw.mydb.util.ConsistentHash;

/**
 * 根据给定值的HASH来分库，底层hash算法使用guava的murmurHash3。
 * 不考虑再映射问题了。
 *
 * @author axeon
 */
public class RouteDatabaseByHash extends RouteAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(RouteDatabaseByHash.class);

    /**
     * 一致性hash对象。
     */
    private ConsistentHash<DataNode> consistentHash = null;

    @Override
    public void config() {
        //构造一致性hash。
        this.consistentHash = new ConsistentHash<>(128, dataNodes);
    }

    @Override
    public RouteInfo calculate(MydbConfig.TableConfig tableConfig, RouteInfo routeInfo, String value) {
        DataNode node = consistentHash.get(value);
        routeInfo.setMysqlGroup(node.getMysqlGroup());
        routeInfo.setDatabase(node.getDatabase());
        return routeInfo;
    }

}