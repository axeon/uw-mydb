package uw.mydb.mysql;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 处理mysql端的数据交互。
 *
 * @author axeon
 */
public class MySqlDataHandler extends ChannelInboundHandlerAdapter {

    public static final AttributeKey<MySqlSession> MYSQL_SESSION = AttributeKey.valueOf("mysql.session");
    private static final Logger logger = LoggerFactory.getLogger(MySqlDataHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        MySqlSession session = ctx.channel().attr(MYSQL_SESSION).get();
        if (session == null) {
            logger.error("MySqlSession未获取到！");
            ctx.close();
            return;
        }
        //拿到消息
        ByteBuf buf = (ByteBuf) msg;
        switch (session.getState()) {
            case MySqlSession.STATE_USING:
                //开始接受业务数据。
                session.handleCommandResponse(ctx, buf);
                break;
            case MySqlSession.STATE_NORMAL:
                //闲置idle接收到的信息
                logger.warn("!!!未处理信息:" + ByteBufUtil.prettyHexDump(buf));
                break;
            case MySqlSession.STATE_AUTH:
                //验证阶段。
                session.handleAuthResponse(ctx, buf);
                break;
            case MySqlSession.STATE_INIT:
                //初始阶段，此时需要发送验证包
                session.handleInitResponse(ctx, buf);
                break;
            case MySqlSession.STATE_REMOVED:
                //验证失败信息，直接关闭链接吧。
                ctx.close();
                break;
            default:
                ctx.close();
                //这时候基本上就是登录失败了，直接关连接就好了。
        }
        super.channelRead(ctx, msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        MySqlSession session = ctx.channel().attr(MYSQL_SESSION).get();
        ctx.close();
        session.trueClose();
    }

    /**
     * 退出session。
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        MySqlSession session = ctx.channel().attr(MYSQL_SESSION).get();
        session.trueClose();
        super.channelInactive(ctx);
    }


}