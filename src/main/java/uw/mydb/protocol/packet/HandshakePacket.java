package uw.mydb.protocol.packet;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import uw.mydb.util.ByteBufUtils;

/**
 * MySql握手包
 *
 * @author axeon
 */
public class HandshakePacket extends MySqlPacket {

    private static final byte[] FILLER_13 = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    public byte protocolVersion;
    public byte[] serverVersion;
    public long threadId;
    public byte[] seed;
    public int serverCapabilities;
    public byte serverCharsetIndex;
    public int serverStatus;
    public byte[] restOfScrambleBuff;

    public void read(ByteBuf buf) {
        if (buf.readableBytes() < 28) {
            System.out.println(ByteBufUtil.prettyHexDump(buf));
        }

        packetLength = ByteBufUtils.readUB3(buf);
        packetId = buf.readByte();
        protocolVersion = buf.readByte();
        serverVersion = ByteBufUtils.readBytesWithNull(buf);
        threadId = ByteBufUtils.readUB4(buf);
        seed = ByteBufUtils.readBytesWithNull(buf);
        serverCapabilities = ByteBufUtils.readUB2(buf);
        serverCharsetIndex = buf.readByte();
        serverStatus = ByteBufUtils.readUB2(buf);
        buf.skipBytes(FILLER_13.length);
        restOfScrambleBuff = ByteBufUtils.readBytesWithNull(buf);
    }

    @Override
    public void write(ByteBuf buffer) {
        ByteBufUtils.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte(protocolVersion);
        ByteBufUtils.writeWithNull(buffer, serverVersion);
        ByteBufUtils.writeUB4(buffer, threadId);
        ByteBufUtils.writeWithNull(buffer, seed);
        ByteBufUtils.writeUB2(buffer, serverCapabilities);
        buffer.writeByte(serverCharsetIndex);
        ByteBufUtils.writeUB2(buffer, serverStatus);
        buffer.writeBytes(FILLER_13);
        ByteBufUtils.writeWithNull(buffer, restOfScrambleBuff);
    }

    @Override
    public int calcPacketSize() {
        int size = 1;
        size += serverVersion.length;// n
        size += 5;// 1+4
        size += seed.length;// 8
        size += 19;// 1+2+1+2+13
        size += restOfScrambleBuff.length;// 12
        size += 1;// 1
        return size;
    }


    @Override
    protected String getPacketInfo() {
        return "MySQL Handshake Packet";
    }
}
