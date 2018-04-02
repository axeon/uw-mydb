package uw.mydb.mysql;

import io.netty.buffer.ByteBuf;

/**
 * session回调接口。
 *
 * @author axeon
 */
public interface MySqlSessionCallback {

    /**
     * 通知数据。
     *
     * @param packetType
     * @param buf
     */
    void receivePacket(byte packetType, ByteBuf buf);

    /**
     * 通知解绑定。
     */
    void unbind();

}
