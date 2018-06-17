package uw.mydb.route.algorithm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.conf.MydbConfig;
import uw.mydb.route.RouteAlgorithm;
import uw.mydb.util.ConsistentHash;

/**
 * 根据给定值的HASH来分库，底层hash算法使用guava的murmurHash3。
 *  默认128个虚拟节点，建议以8的倍数来分库效果比较好。
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
    public RouteInfo calculate(MydbConfig.TableConfig tableConfig, RouteInfo routeInfo, String value) throws RouteException {
        DataNode node = consistentHash.get(value);
        if (node == null) {
            logger.warn("指定的value:[{}]无法格式化为long!!!", value);
            throw new RouteException("无法匹配hash节点！");
        }
        routeInfo.setMysqlGroup(node.getMysqlGroup());
        routeInfo.setDatabase(node.getDatabase());
        return routeInfo;
    }

}