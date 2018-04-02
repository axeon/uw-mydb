package uw.mydb.protocol.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.util.ByteBufUtils;

import java.util.List;

/**
 * mysql数据包解码器。
 * 为了性能和透传考虑，没有解析成对象。
 *
 * @author axeon
 */
public class MysqlPacketDecoder extends ByteToMessageDecoder {

    private static final Logger logger = LoggerFactory.getLogger(MysqlPacketDecoder.class);

    /**
     * 包头长度
     */
    private final int packetHeaderSize = 4;

    /**
     * 最大包大小。
     */
    private final int maxPacketSize = 16 * 1024 * 1024;

    /**
     * MySql外层结构解包
     *
     * @param ctx
     * @param in
     * @param out
     * @throws Exception
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // 4 bytes:3 length + 1 packetId
        if (in.readableBytes() < packetHeaderSize) {
            return;
        }
        //此处必须要标记初始readerIndex
        in.markReaderIndex();
        //包长度
        int packetLength = ByteBufUtils.readUB3(in);
        // 过载保护
        if (packetLength > maxPacketSize) {
            throw new IllegalArgumentException("Packet size over the limit:" + maxPacketSize);
        }
        //包不全的情况，下次再读
        if (in.readableBytes() < packetLength + 1) {
            // 半包回溯
            in.resetReaderIndex();
            return;
        }
        in.resetReaderIndex();
        int readLength = packetLength + packetHeaderSize;
        // 尝试用zero copy。
        ByteBuf buf = in.readRetainedSlice(readLength);
        out.add(buf);
    }
}
