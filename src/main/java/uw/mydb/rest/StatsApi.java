package uw.mydb.rest;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import uw.mydb.stats.StatsFactory;
import uw.mydb.stats.vo.SqlStatsPair;

import java.util.Map;

/**
 * 统计接口。
 */
@RestController
@RequestMapping("/api/stats")
public class StatsApi {

    /**
     * 获得mydbStats数据。
     */
    @RequestMapping("/server")
    public SqlStatsPair getMydbStats() {
        return StatsFactory.getMyDbStats();
    }

    /**
     * 获得mysql状态统计。
     */
    @RequestMapping("/mysqlStats")
    public Map<String, SqlStatsPair> getMysqlStats() {
        return StatsFactory.getMysqlStatsMap();
    }

    /**
     * 获得客户端l连接信息。
     */
    @RequestMapping("/clients")
    public Map<String, SqlStatsPair> getClients() {
        return StatsFactory.getClientStatsMap();
    }

    /**
     * 获得客户端l连接信息。
     */
    @RequestMapping("/clientStats")
    public Map<String, SqlStatsPair> getClientStats() {
        return StatsFactory.getClientStatsMap();
    }

    /**
     * 获得客户端l连接信息。
     */
    @RequestMapping("/schemaStats")
    public Map<String, SqlStatsPair> getSchemaStats() {
        return StatsFactory.getSchemaStatsMap();

    }


}
