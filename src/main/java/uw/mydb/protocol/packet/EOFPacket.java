package uw.mydb.protocol.packet;

import io.netty.buffer.ByteBuf;
import uw.mydb.util.ByteBufUtils;

/**
 * From ServerConfig To Client, at the end of a series of Field Packets, and at the
 * end of a series of Data Packets.With prepared statements, EOF Packet can also
 * end parameter information, which we'll describe later.
 * Bytes                 Name
 * -----                 ----
 * 1                     field_count, always = 0xfe
 * 2                     warning_count
 * 2                     Status Flags
 *
 * @author axeon
 */
public class EOFPacket extends MySqlPacket {
    public byte packetType = PACKET_EOF;
    public int warningCount;
    public int status = 2;

    @Override
    public void write(ByteBuf buf) {
        ByteBufUtils.writeUB3(buf, calcPacketSize());
        buf.writeByte(packetId);
        buf.writeByte(packetType);
        ByteBufUtils.writeUB2(buf, warningCount);
        ByteBufUtils.writeUB2(buf, status);
    }

    public void read(ByteBuf buf) {
        packetLength = ByteBufUtils.readUB3(buf);
        packetId = buf.readByte();
        packetType = buf.readByte();
        warningCount = ByteBufUtils.readUB2(buf);
        status = ByteBufUtils.readUB2(buf);
    }

    public boolean hasStatusFlag(long flag) {
        return ((this.status & flag) == flag);
    }

    @Override
    public int calcPacketSize() {
        return 5;// 1+2+2;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL EOF Packet";
    }

}
