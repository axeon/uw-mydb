package uw.mydb.mysql;


import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.LoggerFactory;
import uw.mydb.mysql.util.ConcurrentBag;
import uw.mydb.protocol.packet.*;
import uw.mydb.protocol.util.Capabilitie;
import uw.mydb.util.SecurityUtils;
import uw.mydb.util.SystemClock;

import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Mysql的会话实例。
 *
 * @author axeon
 */
public class MySqlSession implements ConcurrentBag.IConcurrentBagEntry {

    /**
     * 结果集初始状态
     */
    public static final int RESULT_INIT = 0;
    /**
     * 结果集开始状态
     */
    public static final int RESULT_START = 1;
    /**
     * 结果集中间状态
     */
    public static final int RESULT_MID = 2;
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MySqlSession.class);
    private static final AtomicIntegerFieldUpdater<MySqlSession> STATE_UPDATER;

    static {
        STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(MySqlSession.class, "state");
    }

    /**
     * 创建时间.
     */
    final long createTime = SystemClock.now();
    /**
     * 对应的channel。
     */
    Channel channel;
    /**
     * 开始使用时间.
     */
    long lastAccess;

    /**
     * 归属的service
     */
    MySqlService mysqlService;

    /**
     * 前端对应的ctx。
     */
    MySqlSessionCallback sessionCallback;

    /**
     * 连接状态。
     */
    private volatile int state = STATE_INIT;

    /**
     * 结果集状态。
     * 0 正常 1 包头状态 2 field状态 3.row data状态
     */
    private int resultStatus = RESULT_INIT;

    public MySqlSession(MySqlService mysqlService, Channel channel) {
        this.mysqlService = mysqlService;
        this.channel = channel;
    }

    /**
     * 生成密码数据。
     *
     * @param pass
     * @param packet
     * @return
     * @throws NoSuchAlgorithmException
     */
    private static byte[] password(String pass, HandshakePacket packet) throws NoSuchAlgorithmException {
        if (pass == null || pass.length() == 0) {
            return null;
        }
        byte[] password = pass.getBytes();
        int sl1 = packet.seed.length;
        int sl2 = packet.restOfScrambleBuff.length;
        byte[] seed = new byte[sl1 + sl2];
        System.arraycopy(packet.seed, 0, seed, 0, sl1);
        System.arraycopy(packet.restOfScrambleBuff, 0, seed, sl1, sl2);
        return SecurityUtils.scramble411(password, seed);
    }

    /**
     * 检测链接可用性。
     *
     * @return
     */
    public boolean isAlive() {
        return true;
    }

    /**
     * 异步执行一条sql。
     *
     * @param buf
     */
    public void exeCommand(ByteBuf buf) {
        channel.writeAndFlush(buf.retain());
    }

    /**
     * 异步执行一条sql。
     *
     * @param command
     */
    public void exeCommand(CommandPacket command) {
        ByteBuf buf = channel.alloc().buffer();
        command.write(buf);
        channel.writeAndFlush(buf);
    }

    /**
     * 绑定到前端session。
     *
     * @param sessionCallback
     */
    public void bind(MySqlSessionCallback sessionCallback) {
        this.sessionCallback = sessionCallback;
        this.lastAccess = SystemClock.now();
    }

    /**
     * 解绑。
     */
    public void unbind() {
        this.sessionCallback.unbind();
        this.sessionCallback = null;
        this.mysqlService.requiteSession(this);
        this.lastAccess = SystemClock.now();
//        logger.info("{} now is unbind!!!", this);
    }

    /**
     * 处理验证返回结果。
     *
     * @param buf
     */
    public void handleAuthResponse(ChannelHandlerContext ctx, ByteBuf buf) {
        byte status = buf.getByte(4);
        switch (status) {
            case MySqlPacket.PACKET_OK:
                setState(STATE_NORMAL);
                this.mysqlService.addSession(this);
                break;
            case MySqlPacket.PACKET_ERROR:
                //报错了，直接关闭吧。
                setState(STATE_REMOVED);
                trueClose();
                break;
            default:
        }
    }

    /**
     * 处理初始返回结果。
     *
     * @param buf
     */
    public void handleInitResponse(ChannelHandlerContext ctx, ByteBuf buf) {
        HandshakePacket handshakePacket = new HandshakePacket();
        handshakePacket.read(buf);
        // 设置字符集编码
        int charsetIndex = (handshakePacket.serverCharsetIndex & 0xff);
        // 发送应答报文给后端
        AuthPacket packet = new AuthPacket();
        packet.packetId = 1;
        packet.clientFlags = Capabilitie.initClientFlags();
        packet.maxPacketSize = 1024 * 1024;
        packet.charsetIndex = charsetIndex;
        packet.user = mysqlService.getConfig().getUser();
        try {
            packet.password = password(mysqlService.getConfig().getPassword(), handshakePacket);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
        }
        packet.writeToChannel(ctx);
        //进入验证模式。
        setState(STATE_AUTH);
    }


    /**
     * 设置结果集开始。
     */
    private void setResultStart() {
        resultStatus = RESULT_START;
//        logger.warn("{} 结果集开始了！", this);
    }


    /**
     * 检查结果集状态。
     */
    private boolean checkResultEnd() {
        if (resultStatus == RESULT_START) {
            resultStatus = RESULT_MID;
//            logger.warn("{} 结果集已经过FIELD了！", this);
            return false;
        } else {
            resultStatus = RESULT_INIT;
//            logger.warn("{} 结果集已经完事儿！", this);
            return true;
        }
    }

    /**
     * 处理命令返回结果。
     *
     * @param buf
     */
    public void handleCommandResponse(ChannelHandlerContext ctx, ByteBuf buf) {
        byte packetId = buf.getByte(3);
        byte status = buf.getByte(4);
        switch (status) {
            case MySqlPacket.PACKET_OK:
                sessionCallback.receivePacket(MySqlPacket.PACKET_OK, buf);
                unbind();
                break;
            case MySqlPacket.PACKET_EOF:
                //此时要判断resultStatus，确定结束才可以解绑
                sessionCallback.receivePacket(MySqlPacket.PACKET_EOF, buf);
                if (checkResultEnd()) {
                    EOFPacket eof = new EOFPacket();
                    eof.read(buf);
                    //确定没有更多数据了，再解绑，此处可能有问题！
                    if (!eof.hasStatusFlag(MySqlPacket.SERVER_MORE_RESULTS_EXISTS)) {
                        unbind();
                    } else {
                        resultStatus = RESULT_INIT;
                    }
                }
                break;
            case MySqlPacket.PACKET_ERROR:
                //直接转发走
                sessionCallback.receivePacket(MySqlPacket.PACKET_ERROR, buf);
                //都报错了，直接解绑
                unbind();
                break;
            default:
                if (resultStatus == RESULT_INIT) {
                    //此时可能是ResultSetHeader
                    setResultStart();
                    sessionCallback.receivePacket(MySqlPacket.PACKET_RESULT_SET_HEADER, buf);
                } else {
                    if (resultStatus == RESULT_START) {
                        //field区
                        sessionCallback.receivePacket(MySqlPacket.PACKET_FIELD_DATA, buf);
                    } else if (resultStatus == RESULT_MID) {
                        //数据区
                        sessionCallback.receivePacket(MySqlPacket.PACKET_ROW_DATA, buf);
                    } else {
                        //未知(可能错包，黏包)。。。转发
                        sessionCallback.receivePacket(MySqlPacket.PACKET_UNKNOW, buf);
                    }
                }
        }
    }

    /**
     * 真正关闭连接。
     */
    public void trueClose() {
        this.channel.close();
    }

    @Override
    public boolean compareAndSet(int expectState, int newState) {
        return STATE_UPDATER.compareAndSet(this, expectState, newState);
    }

    @Override
    public int getState() {
        return STATE_UPDATER.get(this);
    }

    @Override
    public void setState(int newState) {
        STATE_UPDATER.set(this, newState);
    }
}
