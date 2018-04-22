package uw.mydb.mysql;

/**
 * schema统计。
 */
public class SchemaTableStats {


    /**
     * 请求sql执行次数。
     */
    protected volatile int sqlCount;

    /**
     * 查询计数。
     */
    protected volatile int selectCount;

    /**
     * 更新计数。
     */
    protected volatile int updateCount;

    /**
     * 删除计数。
     */
    protected volatile int deleteCount;

    /**
     * 执行成功次数。
     */
    protected volatile int exeSuccessCount;

    /**
     * 执行失败次数。
     */
    protected volatile int exeFailureCount;

    /**
     * 返回行计数。
     */
    protected volatile int exeRowsCount;

    /**
     * 影响行计数。
     */
    protected volatile int effectRowsCount;

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
