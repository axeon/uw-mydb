package uw.mydb.util;


import io.netty.buffer.ByteBuf;

/**
 * ByteBuf工具类。
 *
 * @author axeon
 */
public class ByteBufUtils {

    public static final byte[] NULL_DATA = new byte[0];

    public static int readUB2(ByteBuf buf) {
        int i = buf.readByte() & 0xff;
        i |= (buf.readByte() & 0xff) << 8;
        return i;
    }

    public static int readUB3(ByteBuf buf) {
        int i = buf.readByte() & 0xff;
        i |= (buf.readByte() & 0xff) << 8;
        i |= (buf.readByte() & 0xff) << 16;
        return i;
    }

    public static long readUB4(ByteBuf buf) {
        long l = buf.readByte() & 0xff;
        l |= (buf.readByte() & 0xff) << 8;
        l |= (buf.readByte() & 0xff) << 16;
        l |= (buf.readByte() & 0xff) << 24;
        return l;
    }

    public static long readLong(ByteBuf buf) {
        long l = (long) (buf.readByte() & 0xff);
        l |= (long) (buf.readByte() & 0xff) << 8;
        l |= (long) (buf.readByte() & 0xff) << 16;
        l |= (long) (buf.readByte() & 0xff) << 24;
        l |= (long) (buf.readByte() & 0xff) << 32;
        l |= (long) (buf.readByte() & 0xff) << 40;
        l |= (long) (buf.readByte() & 0xff) << 48;
        l |= (long) (buf.readByte() & 0xff) << 56;
        return l;
    }

    /**
     * this is for the String
     *
     * @param buf
     * @return
     */
    public static long readLength(ByteBuf buf) {
        int length = buf.readByte() & 0xff;
        switch (length) {
            case 251:
                return -1L;
            case 252:
                return readUB2(buf);
            case 253:
                return readUB3(buf);
            case 254:
                return readLong(buf);
            default:
                return length;
        }
    }

    public static final void writeUB2(ByteBuf buf, int i) {
        buf.writeByte((byte) (i & 0xff));
        buf.writeByte((byte) (i >>> 8));
    }

    public static final void writeUB3(ByteBuf buf, int i) {
        buf.writeByte((byte) (i & 0xff));
        buf.writeByte((byte) (i >>> 8));
        buf.writeByte((byte) (i >>> 16));
    }

    public static final void writeInt(ByteBuf buf, int i) {
        buf.writeByte((byte) (i & 0xff));
        buf.writeByte((byte) (i >>> 8));
        buf.writeByte((byte) (i >>> 16));
        buf.writeByte((byte) (i >>> 24));
    }

    public static final void writeFloat(ByteBuf buf, float f) {
        writeInt(buf, Float.floatToIntBits(f));
    }

    public static final void writeUB4(ByteBuf buf, long l) {
        buf.writeByte((byte) (l & 0xff));
        buf.writeByte((byte) (l >>> 8));
        buf.writeByte((byte) (l >>> 16));
        buf.writeByte((byte) (l >>> 24));
    }

    public static final void writeLong(ByteBuf buf, long l) {
        buf.writeByte((byte) (l & 0xff));
        buf.writeByte((byte) (l >>> 8));
        buf.writeByte((byte) (l >>> 16));
        buf.writeByte((byte) (l >>> 24));
        buf.writeByte((byte) (l >>> 32));
        buf.writeByte((byte) (l >>> 40));
        buf.writeByte((byte) (l >>> 48));
        buf.writeByte((byte) (l >>> 56));
    }

    public static final void writeDouble(ByteBuf buf, double d) {
        writeLong(buf, Double.doubleToLongBits(d));
    }

    public static final void writeLength(ByteBuf buf, long l) {
        if (l < 251) {
            buf.writeByte((byte) l);
        } else if (l < 0x10000L) {
            buf.writeByte((byte) 252);
            writeUB2(buf, (int) l);
        } else if (l < 0x1000000L) {
            buf.writeByte((byte) 253);
            writeUB3(buf, (int) l);
        } else {
            buf.writeByte((byte) 254);
            writeLong(buf, l);
        }
    }

    public static final void writeWithNull(ByteBuf buf, byte[] src) {
        if (src != null) {
            buf.writeBytes(src);
        }
        buf.writeByte((byte) 0);
    }

    public static final void writeWithLength(ByteBuf buf, byte[] src) {
        int length = src.length;
        if (length < 251) {
            buf.writeByte((byte) length);
        } else if (length < 0x10000L) {
            buf.writeByte((byte) 252);
            writeUB2(buf, length);
        } else if (length < 0x1000000L) {
            buf.writeByte((byte) 253);
            writeUB3(buf, length);
        } else {
            buf.writeByte((byte) 254);
            writeLong(buf, length);
        }
        buf.writeBytes(src);
    }


    public static final int getLength(long length) {
        if (length < 251) {
            return 1;
        } else if (length < 0x10000L) {
            return 3;
        } else if (length < 0x1000000L) {
            return 4;
        } else {
            return 9;
        }
    }

    public static final int getLength(byte[] src) {
        int length = src.length;
        if (length < 251) {
            return 1 + length;
        } else if (length < 0x10000L) {
            return 3 + length;
        } else if (length < 0x1000000L) {
            return 4 + length;
        } else {
            return 9 + length;
        }
    }

    public static String readStringWithNull(ByteBuf buf) {
        return new String(readBytesWithNull(buf));
    }

    public static byte[] readBytesWithLength(ByteBuf buf) {
        int length = (int) readLength(buf);
        if (length <= 0) {
            return NULL_DATA;
        }
        byte[] data = new byte[length];
        buf.readBytes(data);
        return data;
    }

    /**
     * 读取以0x00结尾的ByteBuf
     *
     * @param buf
     * @return
     */
    public static byte[] readBytesWithNull(ByteBuf buf) {
        int start = buf.readerIndex();
        while (buf.readableBytes() > 0) {
            if (buf.readByte() == 0x00) {
                break;
            }
        }
        int end = buf.readerIndex();
        if (end - start < 1) {
            return NULL_DATA;
        }
        byte[] data = new byte[end - start - 1];
        buf.readerIndex(start);
        buf.readBytes(data);
        buf.skipBytes(1);
        return data;
    }

}