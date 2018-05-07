package uw.mydb.metric.vo;

import java.util.concurrent.atomic.AtomicLong;

/**
 * sql统计信息。
 *
 * @author axeon
 */
public class SqlStats {


    /**
     * 读请求sql执行次数。
     */
    protected AtomicLong sqlReadCount = new AtomicLong();

    /**
     * 写请求sql执行次数。
     */
    protected AtomicLong sqlWriteCount = new AtomicLong();

    /**
     * 执行成功次数。
     */
    protected AtomicLong exeSuccessCount = new AtomicLong();

    /**
     * 执行失败次数。
     */
    protected AtomicLong exeFailureCount = new AtomicLong();

    /**
     * 数据行计数。
     */
    protected AtomicLong dataRowsCount = new AtomicLong();

    /**
     * 受影响行计数。
     */
    protected AtomicLong affectRowsCount = new AtomicLong();

    /**
     * 执行消耗时间。
     */
    protected AtomicLong exeTime = new AtomicLong();

    /**
     * 发送字节数。
     */
    protected AtomicLong sendBytes = new AtomicLong();

    /**
     * 接收字节数。
     */
    protected AtomicLong recvBytes = new AtomicLong();


    public void addSqlReadCount(long sqlCount) {
        this.sqlReadCount.addAndGet(sqlCount);
    }

    public void addSqlWriteCount(long sqlCount) {
        this.sqlWriteCount.addAndGet(sqlCount);
    }

    public void addExeSuccessCount(long exeSuccessCount) {
        this.exeSuccessCount.addAndGet(exeSuccessCount);
    }

    public void addExeFailureCount(long exeFailureCount) {
        this.exeFailureCount.addAndGet(exeFailureCount);
    }

    public void addDataRowsCount(long dataRowsCount) {
        this.dataRowsCount.addAndGet(dataRowsCount);
    }

    public void addAffectRowsCount(long affectRowsCount) {
        this.affectRowsCount.addAndGet(affectRowsCount);
    }

    public void addExeTime(long exeTime) {
        this.exeTime.addAndGet(exeTime);
    }

    public void addSendBytes(long sendBytes) {
        this.sendBytes.addAndGet(sendBytes);
    }

    public void addRecvBytes(long recvBytes) {
        this.recvBytes.addAndGet(recvBytes);
    }

    public long getSqlReadCount() {
        return sqlReadCount.get();
    }

    public long getAndClearSqlReadCount() {
        return sqlReadCount.getAndSet(0);
    }


    public long getSqlWriteCount() {
        return sqlWriteCount.get();
    }

    public long getAndClearSqlWriteCount() {
        return sqlWriteCount.getAndSet(0);
    }

    public long getDataRowsCount() {
        return dataRowsCount.get();
    }

    public long getAndClearDataRowsCount() {
        return dataRowsCount.getAndSet(0);
    }

    public long getAffectRowsCount() {
        return affectRowsCount.get();
    }

    public long getAndClearAffectRowsCount() {
        return affectRowsCount.getAndSet(0);
    }

    public long getExeSuccessCount() {
        return exeSuccessCount.get();
    }

    public long getAndClearExeSuccessCount() {
        return exeSuccessCount.getAndSet(0);
    }

    public long getAndClearExeFailureCount() {
        return exeFailureCount.getAndSet(0);
    }

    public long getExeFailureCount() {
        return exeFailureCount.get();
    }

    public long getExeTime() {
        return exeTime.get();
    }

    public long getAndClearExeTime() {
        return exeTime.getAndSet(0);
    }

    public long getSendBytes() {
        return sendBytes.get();
    }

    public long getAndClearSendBytes() {
        return sendBytes.getAndSet(0);
    }

    public long getRecvBytes() {
        return recvBytes.get();
    }

    public long getAndClearRecvBytes() {
        return recvBytes.getAndSet(0);
    }
}
