package uw.mydb.proxy;


import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.mysql.MySqlGroupManager;
import uw.mydb.mysql.MySqlGroupService;
import uw.mydb.mysql.MySqlSessionCallback;
import uw.mydb.protocol.packet.ErrorPacket;
import uw.mydb.protocol.packet.MySqlPacket;
import uw.mydb.protocol.packet.OKPacket;
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
        countDownLatch = new CountDownLatch(routeResult.getSqlInfos().size());
    }

    /**
     * 转发后端的数据包。
     */
    @Override
    public void receivePacket(byte packetType, ByteBuf buf) {
        //解析数据包。。。
        switch (packetType) {
            case MySqlPacket.PACKET_OK:
                OKPacket okPacket = new OKPacket();
                okPacket.read(buf);
                affectedRows += okPacket.affectedRows;
                break;
            case MySqlPacket.PACKET_FIELD_DATA:
                break;
            case MySqlPacket.PACKET_ROW_DATA:
                break;
            case MySqlPacket.PACKET_ERROR:
                ErrorPacket errorPacket = new ErrorPacket();
                errorPacket.read(buf);
//                errorMessage = errorPacket.message;
                break;
            default:
                break;

        }
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
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error(e.getLocalizedMessage(), e);
        }
        //此处汇聚输出。
    }
}
