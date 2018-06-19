package uw.mydb.route.algorithm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.conf.MydbConfig;
import uw.mydb.route.RouteAlgorithm;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * 按照prefix+value值来路由到指定库。
 * 参数：prefix:库名前缀。
 *
 * @author axeon
 */
public class RouteDatabaseByKey extends RouteAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(RouteDatabaseByKey.class);

    /**
     * 库名前缀
     */
    private String prefix;

    /**
     * datanode映射表，key=prefix+value/库名
     */
    private Map<String, DataNode> dataNodeMap = null;

    @Override
    public void config() {
        //获得前缀数据
        Map<String, String> params = algorithmConfig.getParams();
        prefix = params.getOrDefault("prefix", "");
        //做好map映射表。
        dataNodeMap = dataNodes.stream().collect(Collectors.toMap(DataNode::getDatabase, dn -> dn));
    }

    @Override
    public RouteInfo calculate(MydbConfig.TableConfig tableConfig, RouteInfo routeInfo, String value) throws RouteException {
        DataNode dataNode = dataNodeMap.get(prefix + value);
        if (dataNode == null) {
            logger.error("calculate分库计算失败，参数值[{}]错误！", value);
            throw new RouteException("calculate分库计算失败，参数值[" + value + "]错误！");
        }
        routeInfo.setDataNode(dataNode);
        return routeInfo;
    }

}
