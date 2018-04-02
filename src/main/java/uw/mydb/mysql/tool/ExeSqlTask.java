package uw.mydb.mysql.tool;

import io.netty.buffer.ByteBuf;
import uw.mydb.protocol.packet.ErrorPacket;
import uw.mydb.protocol.packet.MySqlPacket;
import uw.mydb.protocol.packet.OKPacket;

/**
 * 执行数据库操作的任务。
 * 一般对应insert,update,delete，只返回affect rows。
 *
 * @author axeon
 */
public class ExeSqlTask extends LocalTaskAdapter<Long> {

    public ExeSqlTask(String mysqlGroupName, LocalCmdCallback<Long> localCmdCallback) {
        super(mysqlGroupName, localCmdCallback);
    }


    /**
     * 处理返回结果。
     *
     * @param packetType
     * @param buf
     */
    @Override
    public void receivePacket(byte packetType, ByteBuf buf) {
        //解析数据包。。。
        switch (packetType) {
            case MySqlPacket.PACKET_OK:
                OKPacket okPacket = new OKPacket();
                okPacket.read(buf);
                data = okPacket.affectedRows;
                break;
            case MySqlPacket.PACKET_ERROR:
                ErrorPacket errorPacket = new ErrorPacket();
                errorPacket.read(buf);
                errorMessage = errorPacket.message;
                break;
            default:
                break;
        }
    }
}
