package uw.mydb.protocol.packet;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MySqlPacket
 *
 * @author axeon
 */
public abstract class MySqlPacket {

    private static final Logger logger = LoggerFactory.getLogger(MySqlPacket.class);


    // 后端报文类型
    public static final byte PACKET_OK = 0;
    public static final byte PACKET_ERROR = (byte) 0xFF;
    public static final byte PACKET_EOF = (byte) 0xFE;
    public static final byte PACKET_AUTH = 1;
    public static final byte PACKET_QUIT = 2;


    /**
     * 当前为load data的响应包
     */
    public static final byte LOAD_DATA_PACKET = (byte) 0xfb;

    /**
     * none, this is an internal thread state
     */
    public static final byte CMD_SLEEP = 0;

    // 前端报文类型
    /**
     * mysql_close
     */
    public static final byte CMD_QUIT = 1;
    /**
     * mysql_select_db
     */
    public static final byte CMD_INIT_DB = 2;
    /**
     * mysql_real_query
     */
    public static final byte CMD_QUERY = 3;
    /**
     * mysql_list_fields
     */
    public static final byte CMD_FIELD_LIST = 4;
    /**
     * mysql_create_db (deprecated)
     */
    public static final byte CMD_CREATE_DB = 5;
    /**
     * mysql_drop_db (deprecated)
     */
    public static final byte CMD_DROP_DB = 6;
    /**
     * mysql_refresh
     */
    public static final byte CMD_REFRESH = 7;
    /**
     * mysql_shutdown
     */
    public static final byte CMD_SHUTDOWN = 8;
    /**
     * mysql_stat
     */
    public static final byte CMD_STATISTICS = 9;
    /**
     * mysql_list_processes
     */
    public static final byte CMD_PROCESS_INFO = 10;
    /**
     * none, this is an internal thread state
     */
    public static final byte CMD_CONNECT = 11;
    /**
     * mysql_kill
     */
    public static final byte CMD_PROCESS_KILL = 12;
    /**
     * mysql_dump_debug_info
     */
    public static final byte CMD_DEBUG = 13;
    /**
     * mysql_ping
     */
    public static final byte CMD_PING = 14;
    /**
     * none, this is an internal thread state
     */
    public static final byte CMD_TIME = 15;
    /**
     * none, this is an internal thread state
     */
    public static final byte CMD_DELAYED_INSERT = 16;
    /**
     * mysql_change_user
     */
    public static final byte CMD_CHANGE_USER = 17;
    /**
     * used by slave server mysqlbinlog
     */
    public static final byte CMD_BINLOG_DUMP = 18;
    /**
     * used by slave server to get master table
     */
    public static final byte CMD_TABLE_DUMP = 19;
    /**
     * used by slave to log connection to master
     */
    public static final byte CMD_CONNECT_OUT = 20;
    /**
     * used by slave to register to master
     */
    public static final byte CMD_REGISTER_SLAVE = 21;
    /**
     * mysql_stmt_prepare
     */
    public static final byte CMD_STMT_PREPARE = 22;
    /**
     * mysql_stmt_execute
     */
    public static final byte CMD_STMT_EXECUTE = 23;
    /**
     * mysql_stmt_send_long_data
     */
    public static final byte CMD_STMT_SEND_LONG_DATA = 24;
    /**
     * mysql_stmt_close
     */
    public static final byte CMD_STMT_CLOSE = 25;
    /**
     * mysql_stmt_reset
     */
    public static final byte CMD_STMT_RESET = 26;
    /**
     * mysql_set_server_option
     */
    public static final byte CMD_SET_OPTION = 27;
    /**
     * mysql_stmt_fetch
     */
    public static final byte CMD_STMT_FETCH = 28;
    /**
     * mysql_stmt_fetch
     */
    public static final byte CMD_DAEMON = 29;
    /**
     * mysql_stmt_fetch
     */
    public static final byte CMD_BINLOG_DUMP_GTID = 30;
    /**
     * mysql_stmt_fetch
     */
    public static final byte CMD_RESET_CONNECTION = 31;
    /**
     * Mycat heartbeat
     */
    public static final byte CMD_HEARTBEAT = 64;

    /**
     * MORE RESULTS
     */
    public static final int SERVER_MORE_RESULTS_EXISTS = 8;

    /**
     * 包长度
     */
    public int packetLength;

    /**
     * 包ID
     */
    public byte packetId = 0;

    /**
     * 把数据包通过后端连接写出，一般使用buffer机制来提高写的吞吐量。
     */
    public abstract void write(ByteBuf buffer);

    /**
     * 把数据包直接写入ctx。
     *
     * @param ctx
     */
    public void writeToChannel(ChannelHandlerContext ctx) {
        ByteBuf buf = ctx.alloc().buffer();
        write(buf);
        ctx.write(buf);
    }

    /**
     * 把数据包直接写入ctx。
     *
     * @param channel
     */
    public void writeToChannel(Channel channel) {
        ByteBuf buf = channel.alloc().buffer();
        write(buf);
        channel.write(buf);
    }


    /**
     * 计算数据包大小，不包含包头长度。
     */
    public abstract int calcPacketSize();

    /**
     * 取得数据包信息
     */
    protected abstract String getPacketInfo();

    @Override
    public String toString() {
        return new StringBuilder().append(getPacketInfo()).append("{length=").append(packetLength).append(",id=")
                .append(packetId).append('}').toString();
    }

}