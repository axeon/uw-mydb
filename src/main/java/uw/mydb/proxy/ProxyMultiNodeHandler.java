package uw.mydb.proxy;


import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.mysql.MySqlGroupManager;
import uw.mydb.mysql.MySqlGroupService;
import uw.mydb.mysql.MySqlSessionCallback;
import uw.mydb.protocol.packet.OKPacket;
import uw.mydb.protocol.packet.ResultSetHeaderPacket;
import uw.mydb.sqlparser.SqlParseResult;

import java.util.concurrent.CountDownLatch;

/**
 * 前端代理的多节点汇聚处理器。
 * 为了高效处理数据，返回时候不对数据集进行任何处理。
 * 缓存表头，内容部分按照流格式直接输出。
 *
 * @author axeon
 */
public class ProxyMultiNodeHandler implements MySqlSessionCallback, Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ProxyMultiNodeHandler.class);

    /**
     * 绑定的channel
     */
    private Channel channel;

    /**
     * 行数
     */
    private volatile long affectedRows;

    /**
     * 门栓
     */
    private CountDownLatch countDownLatch;

    /**
     * 要查询的mysqlGroups
     */
    private SqlParseResult routeResult;

    public ProxyMultiNodeHandler(Channel channel, SqlParseResult routeResult) {
        this.channel = channel;
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
        affectedRows += okPacket.affectedRows;
        channel.write(buf.retain());
    }

    /**
     * 收到Error数据包。
     *
     * @param buf
     */
    @Override
    public void receiveErrorPacket(byte packetId, ByteBuf buf) {
//        ErrorPacket errorPacket = new ErrorPacket();
//        errorPacket.read(buf);
        channel.write(buf.retain());
    }

    /**
     * 收到ResultSetHeader数据包。
     *
     * @param buf
     */
    @Override
    public void receiveResultSetHeaderPacket(byte packetId, ByteBuf buf) {
        ResultSetHeaderPacket rsp = new ResultSetHeaderPacket();
        rsp.read(buf);
        channel.write(buf.retain());
    }

    /**
     * 收到FieldPacket数据包。
     *
     * @param buf
     */
    @Override
    public void receiveFieldDataPacket(byte packetId, ByteBuf buf) {
        channel.write(buf.retain());

    }

    /**
     * 收到FieldEOFPacket数据包。
     *
     * @param buf
     */
    @Override
    public void receiveFieldDataEOFPacket(byte packetId, ByteBuf buf) {
        channel.write(buf.retain());
    }

    /**
     * 收到RowDataPacket数据包。
     *
     * @param buf
     */
    @Override
    public void receiveRowDataPacket(byte packetId, ByteBuf buf) {
        channel.write(buf.retain());
    }

    /**
     * 收到RowDataEOFPacket数据包。
     *
     * @param buf
     */
    @Override
    public void receiveRowDataEOFPacket(byte packetId, ByteBuf buf) {
        channel.write(buf.retain());
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
            if (routeResult.isMaster()) {
                groupService.getMasterService().getSession(this).exeCommand(sqlInfo.genPacket());
            }
        }
//        try {
//            countDownLatch.await(60, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//            logger.error(e.getLocalizedMessage(), e);
//        }
        //此处汇聚输出。
    }
}
