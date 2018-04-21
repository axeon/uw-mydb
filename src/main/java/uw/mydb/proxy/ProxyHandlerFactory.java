package uw.mydb.proxy;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import uw.mydb.protocol.codec.MysqlPacketDecoder;

/**
 * 前端handler工厂
 *
 * @author axeon
 */
public class ProxyHandlerFactory extends ChannelInitializer<SocketChannel> {

//    static final EventExecutorGroup group = new DefaultEventExecutorGroup(16);

    @Override
    protected void initChannel(SocketChannel ch) {
        // decode mysql packet depend on it's length
//        ch.pipeline().addLast( new LoggingHandler(LogLevel.INFO));
        ch.pipeline().addLast(new MysqlPacketDecoder());
//        ch.pipeline().addLast(group, "ProxyDataHandler", new ProxyDataHandler());
        ch.pipeline().addLast(new ProxyDataHandler());
    }
}