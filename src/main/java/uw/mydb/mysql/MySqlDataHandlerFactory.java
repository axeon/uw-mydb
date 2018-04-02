package uw.mydb.mysql;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import uw.mydb.protocol.codec.MysqlPacketDecoder;

/**
 * 前端handler工厂
 *
 * @author axeon
 */
public class MySqlDataHandlerFactory extends ChannelInitializer<SocketChannel> {

//    static final EventExecutorGroup group = new DefaultEventExecutorGroup(16);

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline().addLast(new MysqlPacketDecoder());
//        ch.pipeline().addLast(group,"MysqlDataHander",new MySqlDataHandler());
        ch.pipeline().addLast(new MySqlDataHandler());


    }

}