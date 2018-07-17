package uw.mydb.rest;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import uw.mydb.stats.StatsManager;
import uw.mydb.stats.vo.MySqlRunInfo;
import uw.mydb.stats.vo.ServerRunInfo;
import uw.mydb.stats.vo.SqlStats;
import uw.mydb.stats.vo.SqlStatsPair;

import java.util.Map;

/**
 * 统计接口。
 * @author axeon
 */
@RestController
@RequestMapping("/api/stats")
public class StatsApi {

    /**
     * 获得mydbStats数据。
     */
    @RequestMapping("/sql/server")
    public SqlStatsPair getMydbStats() {
        return StatsManager.getServerSqlStats();
    }

    /**
     * 获得客户端l连接信息。
     */
    @RequestMapping("/sql/client")
    public Map<String, SqlStatsPair> getClients() {
        return StatsManager.getClientSqlStatsMap();
    }

    /**
     * 获得schema信息。
     */
    @RequestMapping("/sql/schema")
    public Map<String, SqlStatsPair> getSchemaStats() {
        return StatsManager.getSchemaSqlStatsMap();
    }

    /**
     * 获得mysql状态统计。
     */
    @RequestMapping("/sql/mysql")
    public Map<String, SqlStats> getMysqlStats() {
        return StatsManager.getMysqlSqlStatsMap();
    }

    /**
     * 获得服务器运行信息。
     */
    @RequestMapping("/run/server")
    public ServerRunInfo getServerRunInfo() {
        return StatsManager.getServerRunStats();
    }

    /**
     * 获得mysql运行信息。
     */
    @RequestMapping("/run/mysql")
    public Map<String, MySqlRunInfo> getMySqlRunInfos() {
        return StatsManager.getMySqlServiceStats();
    }

}
