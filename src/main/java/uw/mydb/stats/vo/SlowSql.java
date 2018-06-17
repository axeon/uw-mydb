package uw.mydb.stats.vo;

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
     * 路由大小。
     */
    private int routeSize;

    /**
     * 数据行计数。
     */
    protected int rowsCount;
    /**
     * 发送字节数。
     */
    protected long sendBytes;
    /**
     * 接收字节数。
     */
    protected long recvBytes;

    /**
     * 执行毫秒数。
     */
    private long exeTime;
    /**
     * 执行时间。
     */
    private long exeDate;

    public SlowSql(String client, String schema, String sql, int routeSize, int rowsCount, long sendBytes, long recvBytes, long exeTime, long exeDate) {
        this.client = client;
        this.schema = schema;
        this.sql = sql;
        this.routeSize = routeSize;
        this.rowsCount = rowsCount;
        this.sendBytes = sendBytes;
        this.recvBytes = recvBytes;
        this.exeTime = exeTime;
        this.exeDate = exeDate;
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

    public int getRouteSize() {
        return routeSize;
    }

    public void setRouteSize(int routeSize) {
        this.routeSize = routeSize;
    }

    public int getRowsCount() {
        return rowsCount;
    }

    public void setRowsCount(int rowsCount) {
        this.rowsCount = rowsCount;
    }

    public long getSendBytes() {
        return sendBytes;
    }

    public void setSendBytes(long sendBytes) {
        this.sendBytes = sendBytes;
    }

    public long getRecvBytes() {
        return recvBytes;
    }

    public void setRecvBytes(long recvBytes) {
        this.recvBytes = recvBytes;
    }

    public long getExeTime() {
        return exeTime;
    }

    public void setExeTime(long exeTime) {
        this.exeTime = exeTime;
    }

    public long getExeDate() {
        return exeDate;
    }

    public void setExeDate(long exeDate) {
        this.exeDate = exeDate;
    }
}
