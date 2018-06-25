package uw.mydb;

import org.springframework.boot.SpringApplication;
import org.springframework.cloud.client.SpringCloudApplication;
import uw.mydb.mysql.MySqlGroupManager;
import uw.mydb.proxy.ProxyServer;
import uw.mydb.route.RouteManager;
import uw.mydb.route.SchemaCheckService;

@SpringCloudApplication
public class UwMydbApplication {

    public static void main(String[] args) throws InterruptedException {
        SpringApplication.run(UwMydbApplication.class, args);
        //启动mysql后端服务器集群。
        MySqlGroupManager.init();
        //启动mysql group心跳
        MySqlGroupManager.start();
        //初始化路由管理器。
        RouteManager.init();
        //schema检查服务开启。
        SchemaCheckService.start();
        //代理服务器启动
        ProxyServer.start();

    }

}
