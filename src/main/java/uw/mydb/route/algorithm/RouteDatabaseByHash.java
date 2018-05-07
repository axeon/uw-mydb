package uw.mydb.route.algorithm;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.route.RouteAlgorithm;

/**
 * 根据给定值的HASH来分库，底层hash算法使用guava的murmurHash3。
 *
 * @author axeon
 */
public class RouteDatabaseByHash extends RouteAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(RouteDatabaseByHash.class);

    private HashFunction hashFunction;

    @Override
    public void config() {
        hashFunction = Hashing.murmur3_32();
    }

    @Override
    public RouteInfo calculate(String tableName, RouteInfo routeInfo, String value) {
        if (routeInfo == null) {
            routeInfo = RouteInfo.newDataWithTable(tableName);
        }
        int hash = hashFunction.hashUnencodedChars(value).asInt();

        return routeInfo;
    }

}