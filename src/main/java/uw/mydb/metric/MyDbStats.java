package uw.mydb.metric;

/**
 * mysql服务统计数据。
 */
public class MyDbStats {

    /**
     * 所有连接数。
     */
    protected volatile int totalConnections;

    /**
     * 请求sql执行次数。
     */
    protected volatile int sqlCount;

    /**
     * 执行成功次数。
     */
    protected volatile int exeSuccessCount;

    /**
     * 执行失败次数。
     */
    protected volatile int exeFailureCount;

    /**
     * 执行消耗时间。
     */
    protected volatile int exeTime;

    /**
     * 发送字节数。
     */
    protected volatile long sendBytes;

    /**
     * 接收字节数。
     */
    protected volatile long recvBytes;

}