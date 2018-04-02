package uw.mydb.protocol.packet;

import io.netty.buffer.ByteBuf;
import uw.mydb.util.ByteBufUtils;

/**
 * From ServerConfig To Client, part of Result Set Packets. One for each column in the
 * result set. Thus, if the value of field_columns in the Result Set Header
 * Packet is 3, then the Field Packet occurs 3 times.
 * Bytes                      Name
 * -----                      ----
 * n (Length Coded String)    catalog
 * n (Length Coded String)    db
 * n (Length Coded String)    table
 * n (Length Coded String)    org_table
 * n (Length Coded String)    name
 * n (Length Coded String)    org_name
 * 1                          (filler)
 * 2                          charsetNumber
 * 4                          length
 * 1                          type
 * 2                          flags
 * 1                          decimals
 * 2                          (filler), always 0x00
 * n (Length Coded Binary)    default
 *
 * @author axeon
 */
public class FieldPacket extends MySqlPacket {
    private static final byte[] DEFAULT_CATALOG = "def".getBytes();
    private static final byte[] FILLER = new byte[2];

    public byte[] catalog = DEFAULT_CATALOG;
    public byte[] db;
    public byte[] table;
    public byte[] orgTable;
    public byte[] name;
    public byte[] orgName;
    public int charsetIndex;
    public long length;
    public int type;
    public int flags;
    public byte decimals;
    public byte[] definition;

    /**
     * 把字节数组转变成FieldPacket
     */
    public void read(ByteBuf buf) {
        packetLength = ByteBufUtils.readUB3(buf);
        packetId = buf.readByte();
        this.catalog = ByteBufUtils.readBytesWithLength(buf);
        this.db = ByteBufUtils.readBytesWithLength(buf);
        this.table = ByteBufUtils.readBytesWithLength(buf);
        this.orgTable = ByteBufUtils.readBytesWithLength(buf);
        this.name = ByteBufUtils.readBytesWithLength(buf);
        this.orgName = ByteBufUtils.readBytesWithLength(buf);
        buf.readByte();
        this.charsetIndex = ByteBufUtils.readUB2(buf);
        this.length = ByteBufUtils.readUB4(buf);
        this.type = buf.readByte() & 0xff;
        this.flags = ByteBufUtils.readUB2(buf);
        this.decimals = buf.readByte();
        buf.skipBytes(FILLER.length);
        if (buf.readableBytes() > 0) {
            this.definition = ByteBufUtils.readBytesWithLength(buf);
        }
    }

    @Override
    public void write(ByteBuf buf) {
        ByteBufUtils.writeUB3(buf, calcPacketSize());
        buf.writeByte(packetId);
        ByteBufUtils.writeWithNull(buf, catalog);
        ByteBufUtils.writeWithNull(buf, db);
        ByteBufUtils.writeWithNull(buf, table);
        ByteBufUtils.writeWithNull(buf, orgTable);
        ByteBufUtils.writeWithNull(buf, name);
        ByteBufUtils.writeWithNull(buf, orgName);
        buf.writeByte((byte) 0x0C);
        ByteBufUtils.writeUB2(buf, charsetIndex);
        ByteBufUtils.writeUB4(buf, length);
        buf.writeByte((byte) (type & 0xff));
        ByteBufUtils.writeUB2(buf, flags);
        buf.writeByte(decimals);
        buf.writeByte((byte) 0x00);
        buf.writeBytes(FILLER);
        if (definition != null) {
            ByteBufUtils.writeWithNull(buf, definition);
        }
    }

    @Override
    public int calcPacketSize() {
        int size = (catalog == null ? 1 : ByteBufUtils.getLength(catalog));
        size += (db == null ? 1 : ByteBufUtils.getLength(db));
        size += (table == null ? 1 : ByteBufUtils.getLength(table));
        size += (orgTable == null ? 1 : ByteBufUtils.getLength(orgTable));
        size += (name == null ? 1 : ByteBufUtils.getLength(name));
        size += (orgName == null ? 1 : ByteBufUtils.getLength(orgName));
        size += 13;// 1+2+4+1+2+1+2
        if (definition != null) {
            size += ByteBufUtils.getLength(definition);
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Field Packet";
    }

}