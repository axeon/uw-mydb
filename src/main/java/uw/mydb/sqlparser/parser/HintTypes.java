package uw.mydb.sqlparser.parser;

/**
 * HINT类型，基于druid的修改版本。
 *
 * @author axeon
 */
public class HintTypes {

    /**
     * mydb hint关键字。
     */
    public static final String MYDB_HINT = "!mydb:";

    /**
     * 数据类型。
     */
    public static final String DB_TYPE = "db-type";

    /**
     * master模式。
     */
    public static final String DB_TYPE_MASTER = "master";

    /**
     * slave模式。
     */
    public static final String DB_TYPE_SLAVE = "slave";

    /**
     * blance模式。
     */
    public static final String DB_TYPE_BALANCE = "balance";

    /**
     * 强行指定路由。
     */
    public static final String ROUTE = "route";


}
