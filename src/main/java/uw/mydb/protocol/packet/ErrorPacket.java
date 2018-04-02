package uw.mydb.protocol.packet;

import io.netty.buffer.ByteBuf;
import uw.mydb.util.ByteBufUtils;

/**
 * From server to client in response to command, if error.
 * Bytes                       Name
 * -----                       ----
 * 1                           field_count, always = 0xff
 * 2                           errorNo
 * 1                           (sqlstate marker), always '#'
 * 5                           sqlstate (5 characters)
 * n                           message
 *
 * @author axeon
 */
public class ErrorPacket extends MySqlPacket {

    private static final byte SQLSTATE_MARKER = (byte) '#';
    private static final byte[] DEFAULT_SQLSTATE = "HY000".getBytes();

    public byte packetType = PACKET_ERROR;
    public int errorNo;
    public byte mark = SQLSTATE_MARKER;
    public byte[] sqlState = DEFAULT_SQLSTATE;
    public String message;

    public void read(ByteBuf buf) {
        packetLength = ByteBufUtils.readUB3(buf);
        packetId = buf.readByte();
        packetType = buf.readByte();
        errorNo = ByteBufUtils.readUB2(buf);
        buf.readByte();
        buf.readBytes(sqlState);
        message = ByteBufUtils.readStringWithNull(buf);
    }

    @Override
    public void write(ByteBuf buf) {
        ByteBufUtils.writeUB3(buf, calcPacketSize());
        buf.writeByte(packetId);
        buf.writeByte(packetType);
        ByteBufUtils.writeUB2(buf, errorNo);
        buf.writeByte(mark);
        buf.writeBytes(sqlState);
        if (message != null) {
            ByteBufUtils.writeWithNull(buf, message.getBytes());
        }
    }

    @Override
    public int calcPacketSize() {
        int size = 9;// 1 + 2 + 1 + 5
        if (message != null) {
            size += message.length() + 1;
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Error Packet";
    }

}
