package uw.mydb.protocol.util;

/**
 * Capabilities标识定义
 *
 * @author axeon
 */
public class Capability {

    /**
     * server capabilities
     *
     * <pre>
     * server:        11110111 11111111
     * client_cmd: 11 10100110 10000101
     * client_jdbc:10 10100010 10001111
     *
     * @see "http://dev.mysql.com/doc/refman/5.1/en/mysql-real-connect.html"
     * </pre>
     */
    // new more secure passwords
    public static final int CLIENT_LONG_PASSWORD = 1;

    // Found instead of affected rows
    // 返回找到（匹配）的行数，而不是改变了的行数。
    public static final int CLIENT_FOUND_ROWS = 2;

    // Get all column flags
    public static final int CLIENT_LONG_FLAG = 4;

    // One can specify db on connect
    public static final int CLIENT_CONNECT_WITH_DB = 8;

    // Don't allow database.table.column
    // 不允许“数据库名.表名.列名”这样的语法。这是对于ODBC的设置。
    // 当使用这样的语法时解析器会产生一个错误，这对于一些ODBC的程序限制bug来说是有用的。
    public static final int CLIENT_NO_SCHEMA = 16;

    /**
     * 使用压缩协议
     */
    public static final int CLIENT_COMPRESS = 32;

    /**
     * Odbc client
     */
    public static final int CLIENT_ODBC = 64;

    // Can use LOAD DATA LOCAL
    public static final int CLIENT_LOCAL_FILES = 128;

    // Ignore spaces before '('
    // 允许在函数名后使用空格。所有函数名可以预留字。
    public static final int CLIENT_IGNORE_SPACE = 256;

    // New 4.1 protocol This is an interactive client
    public static final int CLIENT_PROTOCOL_41 = 512;

    // This is an interactive client
    // 允许使用关闭连接之前的不活动交互超时的描述，而不是等待超时秒数。
    // 客户端的会话等待超时变量变为交互超时变量。
    public static final int CLIENT_INTERACTIVE = 1024;

    // Switch to SSL after handshake
    // 使用SSL。这个设置不应该被应用程序设置，他应该是在客户端库内部是设置的。
    // 可以在调用mysql_real_connect()之前调用mysql_ssl_set()来代替设置。
    public static final int CLIENT_SSL = 2048;

    // IGNORE sigpipes
    // 阻止客户端库安装一个SIGPIPE信号处理器。
    // 这个可以用于当应用程序已经安装该处理器的时候避免与其发生冲突。
    public static final int CLIENT_IGNORE_SIGPIPE = 4096;

    // Client knows about transactions
    public static final int CLIENT_TRANSACTIONS = 8192;

    // Old flag for 4.1 protocol
    public static final int CLIENT_RESERVED = 16384;

    // New 4.1 authentication
    public static final int CLIENT_SECURE_CONNECTION = 32768;

    // Enable/disable multi-stmt support
    // 通知服务器客户端可以发送多条语句（由分号分隔）。如果该标志为没有被设置，多条语句执行。
    public static final int CLIENT_MULTI_STATEMENTS = 65536;

    // Enable/disable multi-results
    // 通知服务器客户端可以处理由多语句或者存储过程执行生成的多结果集。
    // 当打开CLIENT_MULTI_STATEMENTS时，这个标志自动的被打开。
    public static final int CLIENT_MULTI_RESULTS = 131072;


    public static long initClientFlags() {
        int flag = 0;
        flag |= Capability.CLIENT_LONG_PASSWORD;
        flag |= Capability.CLIENT_FOUND_ROWS;
        flag |= Capability.CLIENT_LONG_FLAG;
        flag |= Capability.CLIENT_CONNECT_WITH_DB;
        // flag |= Capability.CLIENT_NO_SCHEMA;
        boolean usingCompress = false;
        if (usingCompress) {
            flag |= Capability.CLIENT_COMPRESS;
        }
        flag |= Capability.CLIENT_ODBC;
        flag |= Capability.CLIENT_LOCAL_FILES;
        flag |= Capability.CLIENT_IGNORE_SPACE;
        flag |= Capability.CLIENT_PROTOCOL_41;
        flag |= Capability.CLIENT_INTERACTIVE;
        // flag |= Capability.CLIENT_SSL;
        flag |= Capability.CLIENT_IGNORE_SIGPIPE;
        flag |= Capability.CLIENT_TRANSACTIONS;
        // flag |= Capability.CLIENT_RESERVED;
        flag |= Capability.CLIENT_SECURE_CONNECTION;
        // client extension
        flag |= Capability.CLIENT_MULTI_STATEMENTS;
        flag |= Capability.CLIENT_MULTI_RESULTS;
        return flag;
    }

    public static int getServerCapabilities() {
        int flag = 0;
        flag |= Capability.CLIENT_LONG_PASSWORD;
        flag |= Capability.CLIENT_FOUND_ROWS;
        flag |= Capability.CLIENT_LONG_FLAG;
        flag |= Capability.CLIENT_CONNECT_WITH_DB;
        // flag |= Capability.CLIENT_NO_SCHEMA;
        // if (usingCompress) {
        // flag |= Capability.CLIENT_COMPRESS;
        // }
        flag |= Capability.CLIENT_ODBC;
        flag |= Capability.CLIENT_LOCAL_FILES;
        flag |= Capability.CLIENT_IGNORE_SPACE;
        flag |= Capability.CLIENT_PROTOCOL_41;
        flag |= Capability.CLIENT_INTERACTIVE;
        // flag |= Capability.CLIENT_SSL;
        flag |= Capability.CLIENT_IGNORE_SIGPIPE;
        flag |= Capability.CLIENT_TRANSACTIONS;
        // flag |= ServerDefs.CLIENT_RESERVED;
        flag |= Capability.CLIENT_SECURE_CONNECTION;
        return flag;
    }
}