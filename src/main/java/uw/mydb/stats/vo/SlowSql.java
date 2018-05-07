package uw.mydb.stats.vo;

import java.util.Date;

/**
 * 慢Sql统计。
 *
 * @author axeon
 */
public class SlowSql {

    /**
     * 发起客户端。
     */
    private String client;

    /**
     * 所在schema 。
     */
    private String schema;

    /**
     * 执行的sql。
     */
    private String sql;

    /**
     * 执行毫秒数。
     */
    private long exeTime;

    /**
     * 执行时间。
     */
    private Date exeDate;

    public SlowSql(String client, String schema, String sql, long exeTime, long exeDate) {
        this.client = client;
        this.schema = schema;
        this.sql = sql;
        this.exeTime = exeTime;
        this.exeDate = new Date(exeDate);
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public long getExeTime() {
        return exeTime;
    }

    public void setExeTime(long exeTime) {
        this.exeTime = exeTime;
    }

    public Date getExeDate() {
        return exeDate;
    }

    public void setExeDate(Date exeDate) {
        this.exeDate = exeDate;
    }
}
