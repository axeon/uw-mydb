package uw.mydb.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * MycatConfig配置管理器。
 * 提供static接口，方便读取配置项。
 *
 * @author axeon
 */
public class MydbConfigManager {

    private static final Logger logger = LoggerFactory.getLogger(MydbConfigManager.class);


    /**
     * DAO配置表.
     */
    private static MydbConfig config = new MydbConfig();


    /**
     * @return the config
     */
    public static MydbConfig getConfig() {
        return config;
    }

    /**
     * 设置配置信息，并整形。
     *
     * @param config the config to set
     */
    public static void setConfig(MydbConfig config) {
        MydbConfigManager.config = config;
        //对配置文件信息进行整形
        for (Map.Entry<String, MydbConfig.MysqlGroupConfig> kv : config.getMysqlGroups().entrySet()) {
            kv.getValue().setName(kv.getKey());
        }
        //给schema赋值
        for (Map.Entry<String, MydbConfig.SchemaConfig> kv : config.getSchemas().entrySet()) {
            MydbConfig.SchemaConfig schemaConfig = kv.getValue();
            schemaConfig.setName(kv.getKey());
            //给表赋值
            for (Map.Entry<String, MydbConfig.TableConfig> tableConfigEntry : kv.getValue().getTables().entrySet()) {
                tableConfigEntry.getValue().setName(tableConfigEntry.getKey());
            }
        }
        //展开dbconfig
        for (Map.Entry<String, MydbConfig.RouteConfig> kv : config.getRoutes().entrySet()) {
            kv.getValue().setName(kv.getKey());
            for (MydbConfig.DataNodeConfig dn : kv.getValue().getDataNodes()) {
                for (String dc : dn.getDbConfig()) {
                    dn.getDatabases().addAll(expandNumList(dc));
                }
            }
        }
        logger.info("mydb config loaded!");
    }

    public static MydbConfig.SchemaConfig getSchemaConfig(String schemaName) {
        return config.getSchemas().get(schemaName);
    }

    /**
     * 获得默认的schema。
     *
     * @return
     */
    public static MydbConfig.SchemaConfig getDefaultSchemaConfig() {
        return config.getSchemas().values().iterator().next();
    }

    /**
     * 根据给定的配置信息，展开一个字符串列表。
     * 配置格式如“db$1-20”标识为db1,db2...db20
     *
     * @param data
     * @return
     */
    private static List<String> expandNumList(String data) {
        List<String> list = new ArrayList<>();
        int pos = data.lastIndexOf('$');
        if (pos > 1) {
            String base = data.substring(0, pos);
            String[] ranges = data.substring(pos + 1).split("-");
            try {
                int start = Integer.parseInt(ranges[0]);
                int end = Integer.parseInt(ranges[1]);
                if (end >= start) {
                    for (; start <= end; start++) {
                        list.add(base + start);
                    }
                }
            } catch (Exception e) {
                logger.error("错误的展开数据：" + data, e);
            }
        } else {
            list.add(data);
        }

        return list;
    }

}
