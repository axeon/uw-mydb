package uw.mydb.stats;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import uw.mydb.conf.MydbConfig;
import uw.mydb.conf.MydbConfigManager;

import java.util.concurrent.TimeUnit;

/**
 * influxDB 操作类
 *
 * @author axeon
 */
@Component
public class InfluxDBService {

    private static final Logger log = LoggerFactory.getLogger(InfluxDBService.class);

    @Bean
    InfluxDB influxDB() {
        MydbConfig.MetricService config = MydbConfigManager.getConfig().getStats().getMetricService();
        InfluxDB influxDB = InfluxDBFactory.connect(config.getHost(), config.getUsername(), config.getPassword());
        influxDB.enableBatch(100, 30, TimeUnit.SECONDS);
        return influxDB;
    }


    /**
     * 更新mydb的sql统计数据。
     */
    public void updateMydbSqlStats() {

    }


}
