package uw.mydb.mysql.tool;

import io.netty.buffer.ByteBuf;
import uw.mydb.mysql.MySqlGroupManager;
import uw.mydb.mysql.MySqlSession;
import uw.mydb.mysql.MySqlSessionCallback;
import uw.mydb.protocol.packet.CommandPacket;
import uw.mydb.protocol.packet.MySqlPacket;

/**
 * 本地任务Adapter。
 *
 * @param <T>
 * @author axeon
 */
public abstract class LocalTaskAdapter<T> implements MySqlSessionCallback {

    /**
     * 用于执行的命令的mysql session。
     */
    protected MySqlSession mysqlSession;

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


    public LocalTaskAdapter(String mysqlGroupName, LocalCmdCallback<T> localCmdCallback) {
        this.mysqlSession = MySqlGroupManager.getMysqlGroupService(mysqlGroupName).getMasterService().getSession(this);
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
        this.mysqlSession.exeCommand(cmd);
    }

    /**
     * 接受的数据包。
     *
     * @param packetType
     * @param buf
     */
    @Override
    public abstract void receivePacket(byte packetType, ByteBuf buf);

    /**
     * 解绑的时候激活回调事件。
     */
    @Override
    public void unbind() {
        if (errorNo == 0) {
            localCmdCallback.onSuccess(data);
        } else {
            localCmdCallback.onFail(errorNo, errorMessage);
        }

    }
}
