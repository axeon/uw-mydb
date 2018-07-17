package uw.mydb.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 统计信息发送服务。
 *
 * @author axeon
 */
@Component
public class StatsSendService {

    private static final Logger log = LoggerFactory.getLogger(StatsSendService.class);

//    @Bean
//    InfluxDB influxDB() {
//        MydbConfig.MetricService config = MydbConfigManager.getConfig().getStats().getMetricService();
//        InfluxDB influxDB = InfluxDBFactory.connect(config.getHost(), config.getUsername(), config.getPassword());
//        influxDB.enableBatch(100, 30, TimeUnit.SECONDS);
//        return influxDB;
//    }

    /**
     * 更新mydb的sql统计数据。
     */
//    @Scheduled(fixedRate=${"uw.mydb.stats.metric-service.interval"})
    public void updateMydbSqlStats() {

    }




}
