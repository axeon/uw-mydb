package uw.mydb.mysql.tool;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.mysql.MySqlGroupManager;
import uw.mydb.mysql.MySqlGroupService;
import uw.mydb.mysql.MySqlSession;
import uw.mydb.mysql.MySqlSessionCallback;
import uw.mydb.protocol.packet.CommandPacket;
import uw.mydb.protocol.packet.ErrorPacket;
import uw.mydb.protocol.packet.MySqlPacket;

/**
 * 本地任务Adapter。
 *
 * @param <T>
 * @author axeon
 */
public abstract class LocalTaskAdapter<T> implements MySqlSessionCallback {

    private static final Logger logger = LoggerFactory.getLogger(LocalTaskAdapter.class);

    /**
     * 用于执行的命令的mysqlGroupName。
     */
    protected String mysqlGroupName;

    /**
     * 本地命令回调。
     */
    protected LocalCmdCallback<T> localCmdCallback;

    /**
     * 要执行的sql指令。
     */
    protected String sql;

    /**
     * 实际数据。
     */
    protected T data;

    /**
     * 列数量。
     */
    protected int fieldCount;

    /**
     * 错误信息。
     */
    protected String errorMessage;

    /**
     * 错误编号
     */
    protected int errorNo;

    /**
     * 是否需要运行master上，一般都是写指令。
     */
    protected boolean isMaster;


    public LocalTaskAdapter(String mysqlGroupName, LocalCmdCallback<T> localCmdCallback) {
        this.mysqlGroupName = mysqlGroupName;
        this.localCmdCallback = localCmdCallback;
    }

    /**
     * 设置sql。
     *
     * @param sql
     */
    public LocalTaskAdapter setSql(String sql) {
        this.sql = sql;
        return this;
    }

    /**
     * 执行sql。
     */
    public void run() {
        CommandPacket cmd = new CommandPacket();
        cmd.command = MySqlPacket.CMD_QUERY;
        cmd.arg = sql.getBytes();

        MySqlGroupService groupService = MySqlGroupManager.getMysqlGroupService(mysqlGroupName);
        if (groupService == null) {
            logger.warn("无法找到合适的mysqlGroup!");
            return;
        }
        MySqlSession mysqlSession = groupService.getMasterService().getSession(this);
        if (mysqlSession == null) {
            logger.warn("无法找到合适的mysqlSession!");
            return;
        }
        mysqlSession.exeCommand(isMaster, cmd);
    }

    /**
     * 收到Ok数据包。
     *
     * @param buf
     */
    @Override
    public void receiveOkPacket(byte packetId, ByteBuf buf) {

    }

    /**
     * 收到Error数据包。
     *
     * @param buf
     */
    @Override
    public void receiveErrorPacket(byte packetId, ByteBuf buf) {

    }

    /**
     * 收到ResultSetHeader数据包。
     *
     * @param buf
     */
    @Override
    public void receiveResultSetHeaderPacket(byte packetId, ByteBuf buf) {

    }

    /**
     * 收到FieldPacket数据包。
     *
     * @param buf
     */
    @Override
    public void receiveFieldDataPacket(byte packetId, ByteBuf buf) {

    }

    /**
     * 收到FieldEOFPacket数据包。
     *
     * @param buf
     */
    @Override
    public void receiveFieldDataEOFPacket(byte packetId, ByteBuf buf) {

    }

    /**
     * 收到RowDataPacket数据包。
     *
     * @param buf
     */
    @Override
    public void receiveRowDataPacket(byte packetId, ByteBuf buf) {

    }

    /**
     * 收到RowDataEOFPacket数据包。
     *
     * @param buf
     */
    @Override
    public void receiveRowDataEOFPacket(byte packetId, ByteBuf buf) {

    }

    /**
     * 错误提示。
     *
     * @param errorNo
     * @param info
     */
    @Override
    public void onFailMessage(int errorNo, String info) {
        //此处不用实现，unbind的时候实现了。
    }

    /**
     * 解绑的时候激活回调事件。
     */
    @Override
    public void onFinish() {
        if (errorNo == 0) {
            localCmdCallback.onSuccess(data);
        } else {
            localCmdCallback.onFail(errorNo, errorMessage);
        }
    }
}
