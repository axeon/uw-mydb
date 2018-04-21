package uw.mydb.proxy;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uw.mydb.conf.GlobalConstants;
import uw.mydb.conf.MydbConfig;
import uw.mydb.conf.MydbConfigManager;
import uw.mydb.mysql.MySqlGroupManager;
import uw.mydb.mysql.MySqlGroupService;
import uw.mydb.mysql.MySqlSession;
import uw.mydb.mysql.MySqlSessionCallback;
import uw.mydb.protocol.packet.*;
import uw.mydb.protocol.util.Capabilitie;
import uw.mydb.protocol.util.ErrorCode;
import uw.mydb.sqlparser.SqlParseResult;
import uw.mydb.sqlparser.SqlParser;
import uw.mydb.util.RandomUtils;
import uw.mydb.util.SecurityUtils;
import uw.mydb.util.SystemClock;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * 代理服务器的会话实体。
 *
 * @author axeon
 */
public class ProxyMysqlSession implements MySqlSessionCallback {

    private static final Logger logger = LoggerFactory.getLogger(ProxyMysqlSession.class);

    /**
     * 全局统一的sessionId生成器。
     */
    private static AtomicLong sessionIdGenerator = new AtomicLong();

    /**
     * 配置文件。
     */
    private static MydbConfig config = MydbConfigManager.getConfig();

    /**
     * 是否已登录。
     */
    private boolean isLogon;

    /**
     * 绑定的channel
     */
    private ChannelHandlerContext ctx;

    /**
     * session Id
     */
    private long id;

    /**
     * 用户名
     */
    private String user;

    /**
     * 连接的主机。
     */
    private String host;

    /**
     * 连接的端口。
     */
    private int port;

    /**
     * 连接的schema。
     */
    private MydbConfig.SchemaConfig schema;

    /**
     * 字符集
     */
    private String charset;

    /**
     * 字符集索引
     */
    private int charsetIndex;

    /**
     * auth验证的seed
     */
    private byte[] authSeed;

    /**
     * 上次访问时间
     */
    private long lastAccess;


    /**
     * 线程池。
     */
    private ThreadPoolExecutor multiNodeExecutor = new ThreadPoolExecutor(10, 100, 20L, TimeUnit.SECONDS, new SynchronousQueue<>(),
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("MutiNodeService-%d").build(), new ThreadPoolExecutor.CallerRunsPolicy());
    ;

    public ProxyMysqlSession(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        this.id = sessionIdGenerator.incrementAndGet();
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean isLogon() {
        return isLogon;
    }

    /**
     * 获得当前的schema。
     *
     * @return
     */
    public MydbConfig.SchemaConfig getSchema() {
        return schema;
    }

    /**
     * 设置schema。
     *
     * @param schemaName
     */
    public void setSchema(String schemaName) {
        MydbConfig.SchemaConfig newSchema = config.getSchemas().get(schemaName);
        if (newSchema != null) {
            this.schema = newSchema;
            MySqlGroupService groupService = MySqlGroupManager.getMysqlGroupService(this.schema.getBaseNode());
            groupService.getMasterService().getSession(this).exeCommand(CommandPacket.build("use " + this.schema.getName()));
        } else {
            //报错，找不到这个schema。
            failMessage(ErrorCode.ER_NO_DB_ERROR, "No database!");
        }
    }

    /**
     * 发送握手包。
     *
     * @param ctx
     */
    public void sendHandshake(ChannelHandlerContext ctx) {
        // 生成认证数据
        byte[] rand1 = RandomUtils.randomBytes(8);
        byte[] rand2 = RandomUtils.randomBytes(12);
        // 保存认证数据
        byte[] seed = new byte[rand1.length + rand2.length];
        System.arraycopy(rand1, 0, seed, 0, rand1.length);
        System.arraycopy(rand2, 0, seed, rand1.length, rand2.length);
        this.authSeed = seed;
        // 发送握手数据包
        HandshakePacket hs = new HandshakePacket();
        hs.packetId = 0;
        hs.protocolVersion = GlobalConstants.PROTOCOL_VERSION;
        hs.serverVersion = GlobalConstants.SERVER_VERSION;
        hs.threadId = id;
        hs.seed = rand1;
        hs.serverCapabilities = Capabilitie.getServerCapabilities();
//        hs.serverCharsetIndex = (byte) (session.charsetIndex & 0xff);
        hs.serverStatus = 2;
        hs.restOfScrambleBuff = rand2;
        //写channel里
        hs.writeToChannel(ctx);
        ctx.flush();
    }

    /**
     * 处理验证包。
     *
     * @param ctx
     * @param buf
     */
    public void auth(ChannelHandlerContext ctx, ByteBuf buf) {
        AuthPacket authPacket = new AuthPacket();
        authPacket.read(buf);
        MydbConfig.UserConfig userConfig = config.getUsers().get(authPacket.user);
        if (userConfig == null) {
            failMessage(ctx, ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + authPacket.user + "', because user is not exists! ");
            ctx.close();
            return;
        }

        //检查用户主机权限。
        boolean isPassed = false;
        List<String> hosts = userConfig.getHosts();
        if ((hosts != null && hosts.size() > 0)) {
            for (String host : hosts) {
                if (Pattern.matches(host, this.host)) {
                    isPassed = true;
                    break;
                }
            }
        }
        if (!isPassed) {
            failMessage(ctx, ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + authPacket.user + "' with address '" + this.ctx + "'");
            ctx.close();
            return;
        }
        // 检查密码匹配。
        byte[] encryptPass = new byte[0];
        try {
            encryptPass = SecurityUtils.scramble411(userConfig.getPassword().getBytes(), this.authSeed);
        } catch (NoSuchAlgorithmException e) {
            logger.warn("no such algorithm", e);
        }
        if (!Arrays.equals(encryptPass, authPacket.password)) {
            failMessage(ctx, ErrorCode.ER_PASSWORD_NO_MATCH, "Access denied for user '" + authPacket.user + "', because password is error ");
            ctx.close();
            return;
        }
        // 检查scheme权限
        List<String> schemas = userConfig.getSchemas();
        if (StringUtil.isNullOrEmpty(authPacket.database) || schemas.contains(authPacket.database)) {
            // set schema
            if (authPacket.database != null) {
                this.schema = config.getSchemas().get(authPacket.database);
            } else {
                //给一个默认的schema。
                this.schema = config.getSchemas().values().iterator().next();
            }
            // 设置字符集编码
            this.charsetIndex = (authPacket.charsetIndex & 0xff);
            //设置session用户
            this.user = authPacket.user;
            this.isLogon = true;
            this.authSeed = null;
            OKPacket.writeAuthOkToChannel(ctx);
        } else {
            String s = "Access denied for user '" + authPacket.user + "' to database '" + authPacket.database + "'";
            failMessage(ctx, ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
            ctx.close();
            return;
        }
    }

    /**
     * 收到Ok数据包。
     *
     * @param buf
     */
    @Override
    public void receiveOkPacket(byte packetId, ByteBuf buf) {
        ctx.write(buf.retain());
    }

    /**
     * 收到Error数据包。
     *
     * @param buf
     */
    @Override
    public void receiveErrorPacket(byte packetId, ByteBuf buf) {
        ctx.write(buf.retain());
    }

    /**
     * 收到ResultSetHeader数据包。
     *
     * @param buf
     */
    @Override
    public void receiveResultSetHeaderPacket(byte packetId, ByteBuf buf) {
        ctx.write(buf.retain());
    }

    /**
     * 收到FieldPacket数据包。
     *
     * @param buf
     */
    @Override
    public void receiveFieldDataPacket(byte packetId, ByteBuf buf) {
        ctx.write(buf.retain());
    }

    /**
     * 收到FieldEOFPacket数据包。
     *
     * @param buf
     */
    @Override
    public void receiveFieldDataEOFPacket(byte packetId, ByteBuf buf) {
        ctx.write(buf.retain());
    }

    /**
     * 收到RowDataPacket数据包。
     *
     * @param buf
     */
    @Override
    public void receiveRowDataPacket(byte packetId, ByteBuf buf) {
        ctx.write(buf.retain());
    }

    /**
     * 收到RowDataEOFPacket数据包。
     *
     * @param buf
     */
    @Override
    public void receiveRowDataEOFPacket(byte packetId, ByteBuf buf) {
        ctx.write(buf.retain());
    }

    /**
     * 切换数据库操作。
     *
     * @param ctx
     * @param buf
     */
    public void initDB(ChannelHandlerContext ctx, ByteBuf buf) {
        CommandPacket cmd = new CommandPacket();
        cmd.read(buf);
        //切换Schema
        this.setSchema(new String(cmd.arg));
    }

    /**
     * handler 查询语句。
     *
     * @param ctx
     * @param buf
     */
    public void query(ChannelHandlerContext ctx, ByteBuf buf) {
        //如果schema没有任何表分区定义，则直接转发到默认库。
        //读取sql
        CommandPacket cmd = new CommandPacket();
        cmd.read(buf);
        String sql = new String(cmd.arg);
        if (logger.isTraceEnabled()) {
            logger.trace("正在执行SQL: {}", sql);
        }
        //进行sql解析
        //根据解析结果判定，当前支持1.单实例执行；2.多实例执行
        SqlParser parser = new SqlParser(this, sql);
        SqlParseResult routeResult = parser.parse();
        //sql解析后，routeResult=null的，可能已经在parser里处理过了。
        if (routeResult.hasError()) {
            //errorcode>0的，发送错误信息。
            if (routeResult.getErrorCode() > 0) {
                failMessage(ctx, routeResult.getErrorCode(), routeResult.getErrorMessage());
            }
            return;
        }
        //压测时，可直接返回ok包的。
        if (routeResult.isSingle()) {
            //单实例执行直接绑定执行即可。
            MySqlGroupService groupService = MySqlGroupManager.getMysqlGroupService(routeResult.getSqlInfo().getMysqlGroup());
            if (groupService == null) {
                failMessage(ctx, ErrorCode.ERR_NO_ROUTE_NODE, "Can't route to mysqlGroup!");
                logger.warn("无法找到合适的mysqlGroup!");
                return;
            }
            MySqlSession mysqlSession = null;
            if (routeResult.isMaster()) {
                mysqlSession = groupService.getMasterService().getSession(this);
            } else {
                mysqlSession = groupService.getLBReadService().getSession(this);
            }
            if (mysqlSession == null) {
                failMessage(ctx, ErrorCode.ERR_NO_ROUTE_NODE, "Can't route to mysqlGroup!");
                logger.warn("无法找到合适的mysqlSession!");
                return;
            }
            mysqlSession.exeCommand(routeResult.getSqlInfo().genPacket());
        } else {
            //多实例执行使用CountDownLatch同步返回所有结果后，再执行转发，可能会导致阻塞。
            new ProxyMultiNodeHandler(this.ctx, routeResult).run();
        }
    }

    /**
     * ping操作。
     *
     * @param ctx
     */
    public void ping(ChannelHandlerContext ctx) {
        OKPacket.writeOkToChannel(ctx);
    }

    /**
     * 前端关闭操作。
     *
     * @param ctx
     */
    public void close(ChannelHandlerContext ctx) {
        ctx.close();
    }

    /**
     * kill操作。
     *
     * @param ctx
     * @param buf
     */
    public void kill(ChannelHandlerContext ctx, ByteBuf buf) {
        failMessage(ctx, ErrorCode.ERR_NOT_SUPPORTED, "NOT SUPPORT OPT");
    }

    /**
     * pstmt预编译。
     *
     * @param ctx
     * @param buf
     */
    public void stmtPrepare(ChannelHandlerContext ctx, ByteBuf buf) {
        failMessage(ctx, ErrorCode.ERR_NOT_SUPPORTED, "NOT SUPPORT OPT");
    }

    /**
     * pstmt执行。
     *
     * @param ctx
     * @param buf
     */
    public void stmtExecute(ChannelHandlerContext ctx, ByteBuf buf) {
        failMessage(ctx, ErrorCode.ERR_NOT_SUPPORTED, "NOT SUPPORT OPT");
    }

    /**
     * pstmt执行关闭。
     *
     * @param ctx
     * @param buf
     */
    public void stmtClose(ChannelHandlerContext ctx, ByteBuf buf) {
        failMessage(ctx, ErrorCode.ERR_NOT_SUPPORTED, "NOT SUPPORT OPT");
    }

    /**
     * 心跳操作。
     *
     * @param ctx
     * @param buf
     */
    public void heartbeat(ChannelHandlerContext ctx, ByteBuf buf) {
        OKPacket.writeOkToChannel(ctx);
    }

    /**
     * 错误提示。
     *
     * @param ctx
     * @param errorNo
     * @param info
     */
    public void failMessage(ChannelHandlerContext ctx, int errorNo, String info) {
        ErrorPacket errorPacket = new ErrorPacket();
        errorPacket.packetId = 1;
        errorPacket.errorNo = errorNo;
        errorPacket.message = info;
        errorPacket.writeToChannel(ctx);
        ctx.flush();
    }


    /**
     * 错误提示。
     *
     * @param errorNo
     * @param info
     */
    public void failMessage(int errorNo, String info) {
        ErrorPacket errorPacket = new ErrorPacket();
        errorPacket.packetId = 1;
        errorPacket.errorNo = errorNo;
        errorPacket.message = info;
        errorPacket.writeToChannel(ctx);
        ctx.flush();
    }

    /**
     * 更新最后访问时间。
     */
    public void updateLastAccess() {
        this.lastAccess = SystemClock.now();
    }

    /**
     * 通知解绑定。
     */
    @Override
    public void unbind() {
        this.ctx.flush();
    }
}
