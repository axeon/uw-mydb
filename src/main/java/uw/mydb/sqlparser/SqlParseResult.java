package uw.mydb.sqlparser;

import org.slf4j.LoggerFactory;
import uw.mydb.protocol.packet.CommandPacket;
import uw.mydb.protocol.packet.MySqlPacket;

import java.util.ArrayList;

/**
 * 最终的路由结果。
 *
 * @author axeon
 */
public class SqlParseResult {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SqlParseResult.class);

    /**
     * 原始的sql语句。
     */
    private String sql;

    /**
     * schema名。
     */
    private String schema;

    /**
     * 表名。
     */
    private String table;

    /**
     * 错误编码。
     */
    private int errorCode;

    /**
     * 错误信息。
     */
    private String errorMessage;

    /**
     * 是否单一路由，默认为true。
     */
    private boolean isSingle = true;

    /**
     * 是否主库操作。
     */
    private Boolean isMaster;

    /**
     * 单sql结果
     */
    private SqlInfo sqlInfo = null;

    /**
     * 多sql结果。
     */
    private ArrayList<SqlInfo> sqlInfos = null;

    public SqlParseResult(String schema, String sql) {
        this.schema = schema;
        this.sql = sql;
    }

    /**
     * 获得sql。
     *
     * @return
     */
    public String getSql() {
        return sql;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    /**
     * 是否有错误。
     *
     * @return
     */
    public boolean hasError() {
        return errorCode != 0;
    }

    /**
     * 设置错误信息。
     *
     * @param errorCode
     * @param errorMessage
     */
    public void setErrorInfo(int errorCode, String errorMessage) {
        if (errorCode > 0) {
            logger.warn("SQL_PARSE_ERR[{}]: {}", errorCode, errorMessage);
        }
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    /**
     * 获得错误码。
     *
     * @return
     */
    public int getErrorCode() {
        return errorCode;
    }

    /**
     * 获得错误信息。
     *
     * @return
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * 是否master路由。
     *
     * @return
     */
    public boolean isMaster() {
        return isMaster;
    }

    /**
     * 设置master路由状态。
     *
     * @param master
     */
    public void setMaster(boolean master) {
        isMaster = master;
    }

    /**
     * 如果未赋值，则设置master状态
     *
     * @param master
     */
    public void setMasterIfNull(boolean master) {
        if (this.isMaster == null) {
            isMaster = master;
        }
    }

    public boolean isSingle() {
        return isSingle;
    }

    public void setSingle(boolean single) {
        isSingle = single;
    }

    public SqlInfo getSqlInfo() {
        return sqlInfo;
    }

    public void setSqlInfo(SqlInfo sqlInfo) {
        this.sqlInfo = sqlInfo;
    }

    public ArrayList<SqlInfo> getSqlInfos() {
        return sqlInfos;
    }

    public void setSqlInfos(ArrayList<SqlInfo> sqlInfos) {
        this.sqlInfos = sqlInfos;
    }

    /**
     * sql信息。
     */
    public static class SqlInfo {

        /**
         * 指定的mysqlGroup.
         */
        private String mysqlGroup;

        /**
         * 主库名。
         */
        private String database;

        /**
         * 主表名。
         */
        private String table;

        /**
         * sql信息。
         */
        private StringBuilder newSqlBuf;

        /**
         * 新的sql。
         */
        private String newSql;

        public SqlInfo(int sqlSize) {
            newSqlBuf = new StringBuilder(sqlSize);
        }

        public SqlInfo(String sql) {
            newSql = sql;
        }

        public String getMysqlGroup() {
            return mysqlGroup;
        }

        public void setMysqlGroup(String mysqlGroup) {
            this.mysqlGroup = mysqlGroup;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getNewSql() {
            if (newSql == null) {
                newSql = newSqlBuf.toString();
            }
            return newSql;
        }

        public SqlInfo appendSql(String text) {
            this.newSqlBuf.append(text);
            return this;
        }

        /**
         * 生成packet。
         *
         * @return
         */
        public CommandPacket genPacket() {
            CommandPacket packet = new CommandPacket();
            packet.command = MySqlPacket.CMD_QUERY;
            packet.arg = getNewSql().getBytes();

            if (logger.isTraceEnabled()) {
                logger.trace("MySQL执行: {}", getNewSql());
            }
            return packet;
        }
    }

}
