package uw.mydb.mysql.tool;

import io.netty.buffer.ByteBuf;
import uw.mydb.protocol.packet.ErrorPacket;
import uw.mydb.protocol.packet.MySqlPacket;
import uw.mydb.protocol.packet.ResultSetHeaderPacket;
import uw.mydb.protocol.packet.RowDataPacket;

import java.util.ArrayList;

/**
 * 所有仅返回单行列表的，都可以使用这个方法。
 * 比如show databases,show tables等。
 *
 * @author axeon
 */
public class SingleListTask extends LocalTaskAdapter<ArrayList<String>> {

    public SingleListTask(String mysqlGroupName, LocalCmdCallback<ArrayList<String>> localCmdCallback) {
        super(mysqlGroupName, localCmdCallback);
        this.data = new ArrayList<>();
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
            case MySqlPacket.PACKET_RESULT_SET_HEADER:
                ResultSetHeaderPacket resultSetHeaderPacket = new ResultSetHeaderPacket();
                resultSetHeaderPacket.read(buf);
                fieldCount = resultSetHeaderPacket.fieldCount;
                break;
            case MySqlPacket.PACKET_ROW_DATA:
                RowDataPacket rowDataPacket = new RowDataPacket(fieldCount);
                rowDataPacket.read(buf);
                data.add(new String(rowDataPacket.fieldValues.get(0)));
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
