package uw.mydb.protocol.packet;

import io.netty.buffer.ByteBuf;
import uw.mydb.util.ByteBufUtils;

/**
 * From server to client after command, if no error and result set -- that is,
 * if the command was a query which returned a result set. The Result Set Header
 * Packet is the first of several, possibly many, packets that the server sends
 * for result sets. The order of packets for a result set is:
 * (Result Set Header Packet)   the number of columns
 * (Field Packets)              column descriptors
 * (EOF Packet)                 marker: end of Field Packets
 * (Row Data Packets)           row contents
 * (EOF Packet)                 marker: end of Data Packets
 * <p>
 * Bytes                        Name
 * -----                        ----
 * 1-9   (Length-Coded-Binary)  field_count
 * 1-9   (Length-Coded-Binary)  extra
 *
 * @author axeon
 */
public class ResultSetHeaderPacket extends MySqlPacket {
    public int fieldCount;
    public long extra;

    public void read(ByteBuf buf) {
        packetLength = ByteBufUtils.readUB3(buf);
        packetId = buf.readByte();
        fieldCount = (int) ByteBufUtils.readLength(buf);
        if (buf.readableBytes() > 0) {
            this.extra = ByteBufUtils.readLength(buf);
        }
    }

    @Override
    public void write(ByteBuf buf) {
        ByteBufUtils.writeUB3(buf, calcPacketSize());
        buf.writeByte(packetId);
        ByteBufUtils.writeLength(buf, fieldCount);
        if (extra > 0) {
            ByteBufUtils.writeLength(buf, extra);
        }
    }

    @Override
    public int calcPacketSize() {
        int size = ByteBufUtils.getLength(fieldCount);
        if (extra > 0) {
            size += ByteBufUtils.getLength(extra);
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL ResultSetHeader Packet";
    }
}