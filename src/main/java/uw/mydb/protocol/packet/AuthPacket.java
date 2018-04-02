package uw.mydb.protocol.packet;

import io.netty.buffer.ByteBuf;
import uw.mydb.protocol.util.Capabilitie;
import uw.mydb.util.ByteBufUtils;

/**
 * From client to server during initial handshake.
 * Bytes                        Name
 * -----                        ----
 * 4                            client_flags
 * 4                            max_packet_size
 * 1                            charset_number
 * 23                           (filler) always 0x00...
 * n (Null-Terminated String)   user
 * n (Length Coded Binary)      scramble_buff (1 + x bytes)
 * n (Null-Terminated String)   databasename (optional)
 *
 * @author axeon
 */
public class AuthPacket extends MySqlPacket {
    private static final byte[] FILLER = new byte[23];

    public long clientFlags;
    public long maxPacketSize;
    public int charsetIndex;
    public byte[] extra;// from FILLER(23)
    public String user;
    public byte[] password;
    public String database;

    public void read(ByteBuf buf) {
        packetLength = ByteBufUtils.readUB3(buf);
        packetId = buf.readByte();
        clientFlags = ByteBufUtils.readUB4(buf);
        maxPacketSize = ByteBufUtils.readUB4(buf);
        charsetIndex = (buf.readByte() & 0xff);
        //跳过filler
        buf.skipBytes(FILLER.length);
        user = ByteBufUtils.readStringWithNull(buf);
        password = ByteBufUtils.readBytesWithLength(buf);
        if (((clientFlags & Capabilitie.CLIENT_CONNECT_WITH_DB) != 0)) {
            database = ByteBufUtils.readStringWithNull(buf);
        }
    }


    @Override
    public void write(ByteBuf buf) {
        // default init 256,so it can avoid buff extract
        ByteBufUtils.writeUB3(buf, calcPacketSize());
        buf.writeByte(packetId);
        ByteBufUtils.writeUB4(buf, clientFlags);
        ByteBufUtils.writeUB4(buf, maxPacketSize);
        buf.writeByte((byte) charsetIndex);
        buf.writeBytes(FILLER);
        if (user == null) {
            buf.writeByte((byte) 0);
        } else {
            byte[] userData = user.getBytes();
            ByteBufUtils.writeWithNull(buf, userData);
        }
        if (password == null) {
            buf.writeByte((byte) 0);
        } else {
            ByteBufUtils.writeWithLength(buf, password);
        }
        if (database == null) {
            buf.writeByte((byte) 0);
        } else {
            byte[] databaseData = database.getBytes();
            ByteBufUtils.writeWithNull(buf, databaseData);
        }
    }

    @Override
    public int calcPacketSize() {
        int size = 32;
        size += (user == null) ? 1 : user.length() + 1;
        size += (password == null) ? 1 : ByteBufUtils.getLength(password);
        size += (database == null) ? 1 : database.length() + 1;
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Authentication Packet";
    }
}
