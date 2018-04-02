package uw.mydb.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * 启动配置文件。
 *
 * @author axeon
 */
@Configuration
@EnableConfigurationProperties({MydbConfig.class})
public class MydbSpringAutoConfiguration {

    /**
     * 日志.
     */
    private static final Logger log = LoggerFactory.getLogger(MydbSpringAutoConfiguration.class);

    /**
     * DAO配置表.
     */
    @Autowired
    private MydbConfig config;


    /**
     * 配置初始化.
     */
    @PostConstruct
    public void init() {
        log.info("uw.mycat start auto configuration...");
        MydbConfigManager.setConfig(config);
        //初始化mysqlgroup服务
        initMysqlGroupService();
    }

    /**
     * 初始化mysql组服务。
     */
    public void initMysqlGroupService() {

    }


    /**
     * 关闭连接管理器,销毁全部连接池.
     */
    @PreDestroy
    public void destroy() {
        log.info("uw.mycat destroy configuration...");
    }
}
