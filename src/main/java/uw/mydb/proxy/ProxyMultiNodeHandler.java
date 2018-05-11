package uw.mydb.proxy;


import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.mysql.MySqlGroupManager;
import uw.mydb.mysql.MySqlGroupService;
import uw.mydb.mysql.MySqlSession;
import uw.mydb.mysql.MySqlSessionCallback;
import uw.mydb.protocol.packet.EOFPacket;
import uw.mydb.protocol.packet.ErrorPacket;
import uw.mydb.protocol.packet.OKPacket;
import uw.mydb.sqlparser.SqlParseResult;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 前端代理的多节点汇聚处理器。
 * 为了高效处理数据，返回时候不对数据集进行任何处理。
 *
 * @author axeon
 */
public class ProxyMultiNodeHandler implements MySqlSessionCallback, Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ProxyMultiNodeHandler.class);

    /**
     * 包阶段：初始。
     */
    private static final int PACKET_STEP_INIT = 0;

    /**
     * 包阶段：字段结束。
     */
    private static final int PACKET_STEP_EOF_FIELD = 1;

    /**
     * 包阶段：全部结束。
     */
    private static final int PACKET_STEP_EOF = 2;

    /**
     * 绑定的channel
     */
    private ChannelHandlerContext ctx;

    /**
     * 行数
     */
    private AtomicLong affectedRows = new AtomicLong(-1);

    /**
     * 门栓
     */
    private CountDownLatch countDownLatch;

    /**
     * packet序列。
     */
    private AtomicInteger packetSeq = new AtomicInteger(0);

    /**
     * 是否已经进入data传输。
     */
    private AtomicInteger packetStep = new AtomicInteger(PACKET_STEP_INIT);

    /**
     * 错误计数。
     */
    private AtomicInteger errorCount = new AtomicInteger(0);

    /**
     * 第一个错误包。
     */
    private ErrorPacket errorPacket = null;

    /**
     * 要查询的mysqlGroups
     */
    private SqlParseResult routeResult;

    /**
     * 数据行计数。
     */
    private AtomicLong dataRowsCount = new AtomicLong();

    /**
     * 执行消耗时间。
     */
    private long exeTime;
    /**
     * 发送字节数。
     */
    private AtomicLong sendBytes = new AtomicLong();
    /**
     * 接收字节数。
     */
    private long recvBytes;

    /**
     * 是否是只读sql
     */
    private boolean isMasterSql = false;

    /**
     * 是否执行失败了
     */
    private boolean isExeSuccess = true;


    public ProxyMultiNodeHandler(ChannelHandlerContext ctx, SqlParseResult routeResult) {
        this.ctx = ctx;
        this.routeResult = routeResult;
        countDownLatch = new CountDownLatch(routeResult.getSqlInfos().size());
    }

    /**
     * 收到Ok数据包。
     *
     * @param buf
     */
    @Override
    public void receiveOkPacket(byte packetId, ByteBuf buf) {
        OKPacket okPacket = new OKPacket();
        okPacket.read(buf);
        if (okPacket.affectedRows > 0) {
            affectedRows.addAndGet(okPacket.affectedRows);
        } else {
            affectedRows.compareAndSet(-1, 0);
        }
    }

    /**
     * 收到Error数据包。
     *
     * @param buf
     */
    @Override
    public void receiveErrorPacket(byte packetId, ByteBuf buf) {
        if (errorCount.compareAndSet(0, 1)) {
            this.errorPacket = new ErrorPacket();
            errorPacket.read(buf);
        }
        errorCount.incrementAndGet();
    }

    /**
     * 收到ResultSetHeader数据包。
     *
     * @param buf
     */
    @Override
    public synchronized void receiveResultSetHeaderPacket(byte packetId, ByteBuf buf) {
        if (packetStep.get() == PACKET_STEP_EOF_FIELD) {
            return;
        }
        if (packetSeq.compareAndSet(0, packetId)) {
            sendBytes.addAndGet(buf.readableBytes());
            ctx.write(buf.retain());
        }
    }

    /**
     * 收到FieldPacket数据包。
     *
     * @param buf
     */
    @Override
    public synchronized void receiveFieldDataPacket(byte packetId, ByteBuf buf) {
        if (packetStep.get() == PACKET_STEP_EOF_FIELD) {
            return;
        }
        if (packetSeq.compareAndSet(packetId - 1, packetId)) {
            sendBytes.addAndGet(buf.readableBytes());
            ctx.write(buf.retain());
        }
    }

    /**
     * 收到FieldEOFPacket数据包。
     *
     * @param buf
     */
    @Override
    public synchronized void receiveFieldDataEOFPacket(byte packetId, ByteBuf buf) {
        if (packetStep.compareAndSet(PACKET_STEP_INIT, PACKET_STEP_EOF_FIELD)) {
            sendBytes.addAndGet(buf.readableBytes());
            ctx.write(buf.retain());
            packetSeq.incrementAndGet();
        }
    }

    /**
     * 收到RowDataPacket数据包。
     *
     * @param buf
     */
    @Override
    public synchronized void receiveRowDataPacket(byte packetId, ByteBuf buf) {
        sendBytes.addAndGet(buf.readableBytes());
        dataRowsCount.incrementAndGet();
        packetId = (byte) (packetSeq.incrementAndGet());
        buf.setByte(3, packetId);
        ctx.write(buf.retain());
    }

    /**
     * 收到RowDataEOFPacket数据包。
     *
     * @param buf
     */
    @Override
    public void receiveRowDataEOFPacket(byte packetId, ByteBuf buf) {
        //在最后汇总输出，可以不用管了。
    }

    /**
     * 通知解绑定。
     */
    @Override
    public void unbind() {
        countDownLatch.countDown();
    }

    @Override
    public void run() {

        for (SqlParseResult.SqlInfo sqlInfo : routeResult.getSqlInfos()) {
            MySqlGroupService groupService = MySqlGroupManager.getMysqlGroupService(sqlInfo.getMysqlGroup());
            if (groupService == null) {
                logger.warn("无法找到合适的mysqlGroup!");
                continue;
            }
            MySqlSession mysqlSession = null;
            if (routeResult.isMaster()) {
                mysqlSession = groupService.getMasterService().getSession(this);
            } else {
                mysqlSession = groupService.getLBReadService().getSession(this);
            }
            if (mysqlSession == null) {
                logger.warn("无法找到合适的mysqlSession!");
                continue;
            }
            mysqlSession.exeCommand(routeResult.isMaster(), sqlInfo);
        }
        //等待最长180s
        try {
            countDownLatch.await(180, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error(e.getLocalizedMessage(), e);
        }
        //如下代码必须要异步线程中跑，否则会出问题。
        //开始返回最后的包。
        if (packetStep.get() > PACKET_STEP_INIT) {
            //输出eof包。
            EOFPacket eofPacket = new EOFPacket();
            eofPacket.packetId = (byte) packetSeq.incrementAndGet();
            eofPacket.warningCount = errorCount.get();
            eofPacket.status = 0x22;
            eofPacket.writeToChannel(ctx);
            sendBytes.addAndGet(eofPacket.calcPacketSize());
        } else {
            if (affectedRows.get() > -1) {
                //说明有ok包。
                OKPacket okPacket = new OKPacket();
                okPacket.packetId = 1;
                okPacket.affectedRows = affectedRows.get();
                okPacket.warningCount = errorCount.get();
                okPacket.writeToChannel(ctx);
                sendBytes.addAndGet(okPacket.calcPacketSize());
            } else {
                //说明全部就是错误包啦，直接返回第一個error包
                errorPacket.writeToChannel(ctx);
                sendBytes.addAndGet(errorPacket.calcPacketSize());
                isExeSuccess = false;
            }
        }
        ctx.flush();
//        StatsFactory.statsMydb();
    }
}
