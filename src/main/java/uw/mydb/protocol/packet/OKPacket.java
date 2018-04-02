package uw.mydb.protocol.packet;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import uw.mydb.util.ByteBufUtils;

/**
 * From ServerConfig To Client, at the end of a series of Field Packets, and at the
 * end of a series of Data Packets.With prepared statements, EOF Packet can also
 * end parameter information, which we'll describe later.
 *
 * <pre>
 * Bytes                 Name
 * -----                 ----
 * 1                     field_count, always = 0xfe
 * 2                     warning_count
 * 2                     Status Flags
 *
 * @author axeon
 */
public class OKPacket extends MySqlPacket {
    public static final byte[] OK = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};
    public static final byte[] AUTH_OK = new byte[]{7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0};

    public byte packetType = PACKET_OK;
    public long affectedRows;
    public long insertId;
    public int serverStatus;
    public int warningCount;
    public byte[] message;

    /**
     * 向通道中写一条ok指令。
     *
     * @param ctx
     */
    public static void writeOkToChannel(ChannelHandlerContext ctx) {
        ByteBuf byteBuf = ctx.alloc().buffer(OKPacket.OK.length).writeBytes(OKPacket.OK);
        ctx.writeAndFlush(byteBuf);
    }

    /**
     * 向通道中写一条ok指令。
     *
     * @param channel
     */
    public static void writeOkToChannel(Channel channel) {
        ByteBuf byteBuf = channel.alloc().buffer(OKPacket.OK.length).writeBytes(OKPacket.OK);
        channel.writeAndFlush(byteBuf);
    }

    /**
     * 向通道中写一条auth ok指令。
     *
     * @param ctx
     */
    public static void writeAuthOkToChannel(ChannelHandlerContext ctx) {
        ByteBuf byteBuf = ctx.alloc().buffer(OKPacket.AUTH_OK.length).writeBytes(OKPacket.AUTH_OK);
        ctx.writeAndFlush(byteBuf);
    }

    @Override
    public void write(ByteBuf buf) {
        ByteBufUtils.writeUB3(buf, calcPacketSize());
        buf.writeByte(packetId);
        ByteBufUtils.writeLength(buf, packetType);
        ByteBufUtils.writeLength(buf, affectedRows);
        ByteBufUtils.writeLength(buf, insertId);
        ByteBufUtils.writeUB2(buf, serverStatus);
        ByteBufUtils.writeUB2(buf, warningCount);
        if (message != null) {
            ByteBufUtils.writeWithLength(buf, message);
        }
    }

    public void read(ByteBuf buf) {
        packetLength = ByteBufUtils.readUB3(buf);
        packetId = buf.readByte();
        packetType = buf.readByte();
        affectedRows = ByteBufUtils.readLength(buf);
        insertId = ByteBufUtils.readLength(buf);
        serverStatus = ByteBufUtils.readUB2(buf);
        warningCount = ByteBufUtils.readUB2(buf);
        if (buf.readableBytes() > 0) {
            message = ByteBufUtils.readBytesWithLength(buf);
        }
    }

    @Override
    public int calcPacketSize() {
        int i = 1;
        i += ByteBufUtils.getLength(affectedRows);
        i += ByteBufUtils.getLength(insertId);
        i += 4;
        if (message != null) {
            i += ByteBufUtils.getLength(message);
        }
        return i;
    }


    @Override
    protected String getPacketInfo() {
        return "MySQL OK Packet";
    }

}
