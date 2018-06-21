package uw.mydb.sqlparser;


import uw.mydb.conf.MydbConfig;
import uw.mydb.protocol.util.ErrorCode;
import uw.mydb.proxy.ProxyMysqlSession;
import uw.mydb.route.RouteAlgorithm;
import uw.mydb.route.RouteManager;
import uw.mydb.sqlparser.parser.HintTypes;
import uw.mydb.sqlparser.parser.Lexer;
import uw.mydb.sqlparser.parser.Token;

import java.util.*;

import static uw.mydb.sqlparser.parser.Token.*;

/**
 * Sql解析器，根据table sharding设置决定分发mysql。
 * 对于常见的select,insert,update,delete等语句会解析sql表信息。
 * 对于其它的sql，需要hint注解指定运行目标，否则仅在base node上执行。
 *
 * @author axeon
 */
public class SqlParser {

    private ProxyMysqlSession proxySession;
    /**
     * sql所在的schema。
     */
    private MydbConfig.SchemaConfig schema;

    /**
     * sql语句。
     */
    private String sql;

    /**
     * lexer解析器。
     */
    private Lexer lexer;

    /**
     * 被分割的子sql。
     */
    private List<String> subSqls = new ArrayList<>();

    /**
     * 插入的多Value列表。
     */
    private List<String> insertValues = null;

    /**
     * 主路由信息。
     */
    private RouteData mainRouteData;

    /**
     * 路由信息Map，除了主路由之外的子表路由。
     */
    private Map<String, RouteData> routeDataMap;

    /**
     * table列表。
     */
    private List<String> tableList;

    /**
     * sql解析结果。
     */
    private SqlParseResult parseResult;

    /**
     * lexer解析位置，用于sql分割。
     */
    private int lexerPos = 0;

    /**
     * 是否是DML.
     */
    private boolean isDML;

    /**
     * 单sql结果。
     */
    private SqlParseResult.SqlInfo sqlInfo;

    /**
     * 多sql结果。
     */
    private ArrayList<SqlParseResult.SqlInfo> sqlInfos = null;

    /**
     * hint的Route信息。
     */
    private String hintRouteInfo = null;

    /**
     * 默认构造器。
     *
     * @param proxySession
     * @param sql
     */
    public SqlParser(ProxyMysqlSession proxySession, String sql) {
        this.proxySession = proxySession;
        this.schema = proxySession.getSchema();
        this.sql = sql;
        this.lexer = new Lexer(sql, false, true);
        this.parseResult = new SqlParseResult(schema.getName(), sql);
    }


    /**
     * 默认构造器。
     *
     * @param schema
     * @param sql
     */
    public SqlParser(MydbConfig.SchemaConfig schema, String sql) {
        this.schema = schema;
        this.sql = sql;
        this.lexer = new Lexer(sql, false, true);
        this.parseResult = new SqlParseResult(schema.getName(), sql);
    }

    /**
     * 解析sql。
     */
    public SqlParseResult parse() {
        if (!lexer.isEOF()) {
            lexer.nextToken();
            //处理注解
            if (lexer.token() == HINT) {
                parseHint(lexer);
                lexer.nextToken();
            }
            switch (lexer.token()) {
                case COMMENT:
                case LINE_COMMENT:
                case MULTI_LINE_COMMENT:
                case SELECT:
                    this.isDML = true;
                    this.parseResult.setMasterIfNull(false);
                    parseSelect(lexer);
                    break;
                case INSERT:
                    this.isDML = true;
                    parseInsert(lexer);
                    break;
                case UPDATE:
                    this.isDML = true;
                    parseUpdate(lexer);
                    break;
                case DELETE:
                    this.isDML = true;
                    parseDelete(lexer);
                    break;
                case REPLACE:
                    this.isDML = true;
                    parseReplace(lexer);
                    break;
                case CREATE:
                    parseCreate(lexer);
                    break;
                case ALTER:
                    parseAlter(lexer);
                    break;
                case DROP:
                    parseDrop(lexer);
                    break;
                case TRUNCATE:
                    parseTruncate(lexer);
                    break;
                case EXPLAIN:
                    parseExplain(lexer);
                    break;
                case USE:
                    parseUse(lexer);
                    break;
                default:
                    if (lexer.token() == SET || lexer.token() == SHOW || lexer.token() == EXPLAIN || lexer.token() == DESCRIBE || lexer.token() == EOF) {
                        //有些类型需要通过虚拟schema上支持的,，这些类型必须可以过。
                    } else {
                        //此时应该是不可用，抛错吧。
                        //剩下的类型都不支持，直接返回报错吧。
                        parseResult.setErrorInfo(ErrorCode.ERR_NOT_SUPPORTED, "NOT SUPPORTED CMD: " + sql);
                    }
                    break;
            }
        }
        //设置table
        if (this.mainRouteData != null && this.mainRouteData.tableConfig != null) {
            this.parseResult.setTable(this.mainRouteData.tableConfig.getName());
        }

        if (!parseResult.hasError()) {
            //如果master未设置，处于保险，设置为true
            this.parseResult.setMasterIfNull(true);
            //计算路由信息。
            calculateRouteInfo();
        }

        if (!parseResult.hasError()) {
            //生成sql。
            generateSqlInfo();
        }

        return parseResult;
    }

    /**
     * 解析use语句。
     *
     * @param lexer
     */
    private void parseUse(Lexer lexer) {
        lexer.check(Token.USE);
        lexer.nextToken();
        if (lexer.token() == Token.IDENTIFIER) {
            //表名
            String database = lexer.stringVal();
            if (proxySession != null) {
                proxySession.setSchema(database);
            }
        }
        //通过设置parseResult，不让cmdQuery再返回数据。
        this.parseResult.setErrorInfo(-1, null);
    }


    /**
     * 解析mydb专有hint。
     *
     * @param lexer
     */
    private void parseHint(Lexer lexer) {
        lexer.check(Token.HINT);
        String hint = lexer.stringVal();
        if (hint.startsWith(HintTypes.MYDB_HINT)) {
            int mark = HintTypes.MYDB_HINT.length();
            int pos = mark;
            //循环解析。
            while (pos < hint.length()) {
                pos = hint.indexOf('=', mark);
                if (pos == -1) {
                    break;
                }
                //属性值
                String name = hint.substring(mark, pos).trim();
                //寻找数值结尾。
                mark = hint.indexOf(';');
                if (mark == -1) {
                    //说明已经到结尾了
                    mark = hint.length();
                }
                String value = hint.substring(pos + 1, mark).trim();
                if (name.equalsIgnoreCase(HintTypes.DB_TYPE)) {
                    //此处处理balance类型。
                    if ("master".equalsIgnoreCase(value)) {
                        parseResult.setMaster(true);
                    } else {
                        parseResult.setMaster(false);
                    }
                } else if (name.equalsIgnoreCase(HintTypes.ROUTE)) {
                    hintRouteInfo = value;
                }
                //进入下一批次处理。
                pos = mark;
            }
        }
    }

    /**
     * 需要强行指定路由，否则仅在baseNode执行。
     *
     * @param lexer
     */
    private void parseTruncate(Lexer lexer) {
        lexer.check(Token.TRUNCATE);
        lexer.nextToken();
        lexer.check(Token.TABLE);
        lexer.nextToken();
        //解析表名
        parseTableName(lexer);
        if (!lexer.isEOF()) {
            lexer.skipToEOF();
        }
        splitSubSql(lexer);
    }

    /**
     * 需要强行指定路由，否则仅在baseNode执行。
     *
     * @param lexer
     */
    private void parseRename(Lexer lexer) {
        lexer.check(Token.RENAME);
        lexer.nextToken();
        lexer.check(Token.TABLE);
        lexer.nextToken();
        if (lexer.token() == Token.IDENTIFIER) {
            //表名
            splitSubSql(lexer);
            String tableName = lexer.stringVal();
            putRouteData(null, tableName, null);
        }
        if (!lexer.isEOF()) {
            lexer.skipToEOF();
        }
        splitSubSql(lexer);
    }

    /**
     * 需要强行指定路由，否则仅在baseNode执行。
     *
     * @param lexer
     */
    private void parseDrop(Lexer lexer) {
        lexer.check(Token.DROP);
        lexer.nextToken();
        //过滤掉TEMPORARY关键字
        if (lexer.token() == Token.TEMPORARY) {
            lexer.nextToken();
        }
        if (lexer.token() == Token.TABLE) {
            lexer.nextToken();
            lexer.skipTo(Token.IDENTIFIER);
            //解析表名
            parseTableName(lexer);
        } else if (lexer.token() == Token.INDEX) {
            lexer.skipTo(Token.ON);
            lexer.nextToken();
            //解析表名
            parseTableName(lexer);
        } else {
            //通过设置parseResult，不让cmdQuery再返回数据。
            this.parseResult.setErrorInfo(ErrorCode.ERR_NOT_SUPPORTED, "NOT SUPPORTED CMD: " + sql);
            return;
        }
        if (!lexer.isEOF()) {
            lexer.skipToEOF();
        }
        splitSubSql(lexer);
    }

    /**
     * 需要强行指定路由，否则仅在baseNode执行。
     *
     * @param lexer
     */
    private void parseAlter(Lexer lexer) {
        lexer.check(Token.ALTER);
        lexer.nextToken();
        if (lexer.token() == Token.TABLE) {
            lexer.nextToken();
            //解析表名
            parseTableName(lexer);
        } else {
            //通过设置parseResult，不让cmdQuery再返回数据。
            this.parseResult.setErrorInfo(ErrorCode.ERR_NOT_SUPPORTED, "NOT SUPPORTED CMD: " + sql);
            return;
        }
        if (!lexer.isEOF()) {
            lexer.skipToEOF();
        }
        splitSubSql(lexer);
    }

    /**
     * 需要强行指定路由，否则仅在baseNode执行。
     *
     * @param lexer
     */
    private void parseCreate(Lexer lexer) {
        lexer.check(Token.CREATE);
        lexer.nextToken();
        //过滤掉TEMPORARY关键字
        if (lexer.token() == Token.TEMPORARY) {
            lexer.nextToken();
        }
        if (lexer.token() == Token.TABLE) {
            lexer.nextToken();
            lexer.skipTo(Token.IDENTIFIER);
            //解析表名
            parseTableName(lexer);
        } else if (lexer.token() == Token.INDEX) {
            lexer.skipTo(Token.ON);
            lexer.nextToken();
            //解析表名
            parseTableName(lexer);
        } else {
            //剩下的类型都不支持，直接返回报错吧。
            //通过设置parseResult，不让cmdQuery再返回数据。
            this.parseResult.setErrorInfo(ErrorCode.ERR_NOT_SUPPORTED, "NOT SUPPORTED CMD: " + sql);
            return;
        }
        if (!lexer.isEOF()) {
            lexer.skipToEOF();
        }
        splitSubSql(lexer);
    }

    /**
     * 需要强行指定路由，否则仅在baseNode执行。
     *
     * @param lexer
     */
    private void parseExplain(Lexer lexer) {
        if (!lexer.isEOF()) {
            lexer.nextToken();
            if (lexer.token() == HINT) {
                parseHint(lexer);
                lexer.nextToken();
            }
            switch (lexer.token()) {
                case HINT:
                case COMMENT:
                case LINE_COMMENT:
                case MULTI_LINE_COMMENT:
                case SELECT:
                    this.parseResult.setMaster(false);
                    parseSelect(lexer);
                    break;
                case INSERT:
                    parseInsert(lexer);
                    break;
                case UPDATE:
                    parseUpdate(lexer);
                    break;
                case DELETE:
                    parseDelete(lexer);
                    break;
                case REPLACE:
                    parseReplace(lexer);
                    break;
                default:
                    break;
            }
            //explain 给schema默认数据。
//            RouteAlgorithm.RouteInfoData routeInfoData = new RouteAlgorithm.RouteInfoData();
//            routeInfoData.setSingle(new RouteAlgorithm.RouteInfo(schema.getBaseNode(), schema.getName(), mainRouteData.tableConfig.getName()));
//            mainRouteData.routeInfoData = routeInfoData;
        }

    }

    /**
     * 解析select语句。
     *
     * @param lexer
     */
    private void parseSelect(Lexer lexer) {
        //直接找到From。
        lexer.skipTo(Token.FROM);
        //解析表内容
        parseTableInfo(lexer);
        if (!checkTableHasRoute()) {
            lexer.skipToEOF();
            splitSubSql(lexer);
            return;
        }
        //跳到where关键字,查找匹配。
        lexer.skipTo(Token.WHERE);
        parseWhereInfo(lexer);

        //截断最后的sql。
        splitSubSql(lexer);
    }

    /**
     * 构造RouteData。
     *
     * @return RouteData
     */
    private RouteData buildRouteData(String table) {
        RouteData routeData = new RouteData();
        routeData.tableConfig = RouteManager.getTableConfig(schema, table);
        //如果不是配置项的，route=null，而且没有keyData
        if (routeData.tableConfig == null) {
            routeData.tableConfig = new MydbConfig.TableConfig();
            routeData.tableConfig.setName(table);
        }
        //如果有route信息的，拉一下routeKeyData。
        if (routeData.tableConfig.getRoute() != null) {
            routeData.routeKeyData = RouteManager.getParamMap(routeData.tableConfig);
        }
        return routeData;
    }

    /**
     * 检查是否数据库表有route匹配
     * 一些情况下，是匹配不到任何table的，这时候就不用匹配keyData了。
     *
     * @return
     */
    private boolean checkTableHasRoute() {
        if (mainRouteData == null || mainRouteData.routeKeyData == null) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * 获得RouteData。
     *
     * @param table
     * @return
     */
    private RouteData getRouteData(String table) {
        RouteData routeData = null;
        if (mainRouteData != null) {
            if (mainRouteData.tableConfig.getName().equals(table)) {
                return mainRouteData;
            }
        }
        if (routeDataMap != null) {
            routeData = routeDataMap.get(table);
            if (routeData != null) {
                return routeData;
            }
        }
        return routeData;
    }

    /**
     * 放置tableConfig。
     */
    private void putRouteData(String schemaName, String tableName, String aliasName) {
        RouteData routeData = getRouteData(tableName);
        //已经有了，直接返回。
        if (routeData != null) {
            return;
        }
        //构造新的
        routeData = buildRouteData(tableName);
        routeData.schemaName = schemaName;
        //优先放mainRouteData
        if (mainRouteData == null) {
            mainRouteData = routeData;
        } else {
            //多表的，放集合内
            if (tableList == null) {
                tableList = new ArrayList<>();
            }
            tableList.add(tableName);
            if (routeDataMap == null) {
                routeDataMap = new HashMap<>();
            }
            routeDataMap.put(tableName, routeData);
        }
        //添加别名
        if (aliasName != null) {
            if (routeDataMap == null) {
                routeDataMap = new HashMap<>();
            }
            routeDataMap.put(aliasName, routeData);
        }
    }


    /**
     * 解析出表名。
     *
     * @param lexer
     */
    private void parseTableName(Lexer lexer) {
        if (lexer.token() == Token.IDENTIFIER) {
            splitSubSql(lexer);
            String schemaName = null;
            String tableName = lexer.stringVal();
            lexer.nextToken();
            if (lexer.token() == Token.DOT) {
                lexer.nextToken();
                //刚刚是库名，现在是表名了
                schemaName = tableName;
                setLexerPos();
                tableName = lexer.stringVal();
                lexer.nextToken();
            }
            putRouteData(schemaName, tableName, null);
        }
    }

    /**
     * 解析insert语句。
     * 先匹配table，然后匹配字段位置。
     * 根据字段位置来取参。
     *
     * @param lexer
     */
    private void parseInsert(Lexer lexer) {
        lexer.check(Token.INSERT);
        lexer.nextToken();
        lexer.check(Token.INTO);
        lexer.nextToken();
        //解析表名
        parseTableName(lexer);
        if (!checkTableHasRoute()) {
            lexer.skipToEOF();
            splitSubSql(lexer);
            return;
        }
        //如果有routeData匹配，采取匹配routeKeyData
        //此时走values的路
        if (lexer.token() == Token.LPAREN) {
            int pos = 0;
            while (!lexer.isEOF()) {
                lexer.nextToken();
                if (lexer.token() == Token.IDENTIFIER) {
                    String colName = lexer.stringVal();
                    RouteAlgorithm.RouteKeyValue routeValue = getRouteKeyValue(colName, null);
                    //设置pos位置
                    if (routeValue != null) {
                        routeValue.setType(pos + 100);
                    }
                } else if (lexer.token() == Token.COMMA) {
                    pos++;
                } else if (lexer.token() == Token.RPAREN) {
                    //可以直接退了
                    break;
                }
            }
            lexer.skipTo(Token.VALUES);
            lexer.nextToken();
            lexer.check(Token.LPAREN);
            lexer.nextToken();
            pos = 0;
            while (!lexer.isEOF()) {
                lexer.nextToken();
                if (lexer.token() == Token.COMMA) {
                    pos++;
                } else if (lexer.token() == Token.RPAREN) {
                    //可以直接退了
                    break;
                } else {
                    if (mainRouteData.routeKeyData.isSingle()) {
                        RouteAlgorithm.RouteKeyValue rkv = mainRouteData.routeKeyData.getValue();
                        if (rkv.getType() == pos + 100) {
                            rkv.putValue(lexer.paramString());
                            //匹配完了，直接退
                            break;
                        }
                    } else {
                        Collection<RouteAlgorithm.RouteKeyValue> rkvs = mainRouteData.routeKeyData.getValues();
                        for (RouteAlgorithm.RouteKeyValue rkv : rkvs) {
                            if (rkv.getType() == pos + 100) {
                                rkv.putValue(lexer.paramString());
                            }
                        }
                    }
                }
            }
        } else if (lexer.token() == Token.SELECT) {
            //如果后面跟着select，则根据select匹配。
        } else {
            //无法解析了。
        }
        if (!lexer.isEOF()) {
            lexer.skipToEOF();
        }
        splitSubSql(lexer);
    }

    /**
     * 解析update语句。
     *
     * @param lexer
     */
    private void parseUpdate(Lexer lexer) {
        lexer.check(Token.UPDATE);
        //解析表内容
        parseTableInfo(lexer);
        if (!checkTableHasRoute()) {
            lexer.skipToEOF();
            splitSubSql(lexer);
            return;
        }
        //跳到where关键字
        lexer.skipTo(Token.WHERE);
        parseWhereInfo(lexer);
        if (!lexer.isEOF()) {
            lexer.skipToEOF();
        }

        splitSubSql(lexer);
    }


    /**
     * 解析replace info语句
     *
     * @param lexer
     */
    private void parseReplace(Lexer lexer) {

    }

    /**
     * 解析delete语句。
     *
     * @param lexer
     */
    private void parseDelete(Lexer lexer) {
        lexer.check(Token.DELETE);
        lexer.nextToken();
        lexer.check(Token.FROM);
        //解析表内容
        parseTableInfo(lexer);
        if (!checkTableHasRoute()) {
            lexer.skipToEOF();
            splitSubSql(lexer);
            return;
        }
        //解析Where
        //跳到where关键字
        lexer.skipTo(Token.WHERE);
        parseWhereInfo(lexer);
        if (!lexer.isEOF()) {
            lexer.skipToEOF();
        }
        splitSubSql(lexer);
    }

    /**
     * 解析Where。
     *
     * @param lexer
     */
    private void parseWhereInfo(Lexer lexer) {
        //开始尝试匹配routeKey
        while (!lexer.isEOF()) {
            lexer.nextToken();
            switch (lexer.token()) {
                case IDENTIFIER:
                    //属性值的情况，检查匹配。
                    String cName1 = lexer.stringVal();
                    String cName2 = null;
                    lexer.nextToken();
                    if (lexer.token() == Token.DOT) {
                        lexer.nextToken();
                    }
                    if (lexer.token() == Token.IDENTIFIER) {
                        cName2 = lexer.stringVal();
                    }
                    RouteAlgorithm.RouteKeyValue routeValue = getRouteKeyValue(cName1, cName2);
                    if (routeValue != null) {
                        //判断操作符，取参数。
                        switch (lexer.token()) {
                            case EQ:
                                lexer.nextToken();
                                routeValue.putValue(lexer.paramString());
                                break;
                            case GT:
                                lexer.nextToken();
                                routeValue.putRangeStart(lexer.paramString());
                                break;
                            case GTEQ:
                                lexer.nextToken();
                                routeValue.putRangeStart(lexer.paramString());
                                break;
                            case LT:
                                lexer.nextToken();
                                routeValue.putRangeEnd(lexer.paramString());
                                break;
                            case LTEQ:
                                lexer.nextToken();
                                routeValue.putRangeEnd(lexer.paramString());
                                break;
                            case BANGEQ:
                                lexer.nextToken();
                                routeValue.putRangeEnd(lexer.paramString());
                                break;
                            case IN:
                                lexer.nextToken();
                                lexer.check(Token.LPAREN);
                                ArrayList<String> vs = new ArrayList<>();
                                while (!lexer.isEOF()) {
                                    lexer.nextToken();
                                    if (lexer.token() == Token.RPAREN) {
                                        break;
                                    } else if (lexer.token() == Token.COMMA) {
                                        break;
                                    } else {
                                        vs.add(lexer.paramString());
                                    }
                                }
                                routeValue.putValues(vs);
                                break;
                            default:
                                break;

                        }
                    }
                    break;
                case SELECT:
                    //里面有嵌套子查询！
                    parseSelect(lexer);
                    break;
                default:
                    //不管了，让他过
                    break;
            }
        }

    }


    /**
     * 解析TableInfo
     *
     * @param lexer
     */
    private void parseTableInfo(Lexer lexer) {
        while (!lexer.isEOF()) {
            lexer.nextToken();
            switch (lexer.token()) {
                case IDENTIFIER:
                    //此处截断sql
                    splitSubSql(lexer);
                    String schemaName = null, tableName = null, aliasName = null;
                    //说明是表名，进入检查。
                    tableName = lexer.stringVal();
                    lexer.nextToken();
                    if (lexer.token() == Token.DOT) {
                        lexer.nextToken();
                        //刚刚是库名，现在是表名了
                        schemaName = tableName;
                        setLexerPos();
                        tableName = lexer.stringVal();
                        lexer.nextToken();
                    }
                    if (lexer.token() == Token.AS) {
                        setLexerPos();
                        lexer.nextToken();
                    }
                    if (lexer.token() == Token.IDENTIFIER) {
                        setLexerPos();
                        aliasName = lexer.stringVal();
                    }
                    //注册表到routeData
                    putRouteData(schemaName, tableName, aliasName);
                    lexer.skipTo(Token.SET, Token.WHERE, Token.JOIN, Token.COMMA);
                    break;
                case JOIN:
                    //此处截断sql
                    splitSubSql(lexer);
                    String schemaJoin = null, tableJoin = null, aliasJoin = null;
                    //说明是表名，进入检查。
                    tableJoin = lexer.stringVal();
                    lexer.nextToken();
                    if (lexer.token() == Token.DOT) {
                        lexer.nextToken();
                        //刚刚是库名，现在是表名了
                        schemaJoin = tableJoin;
                        setLexerPos();
                        tableJoin = lexer.stringVal();
                        lexer.nextToken();
                    }
                    //判断as
                    if (lexer.token() == Token.AS) {
                        setLexerPos();
                        lexer.nextToken();
                    }
                    //判断alias
                    if (lexer.token() == Token.IDENTIFIER) {
                        setLexerPos();
                        aliasJoin = lexer.stringVal();
                    }

                    putRouteData(schemaJoin, tableJoin, aliasJoin);
                    //其他的就跳走吧，不管了。
                    lexer.skipTo(Token.SET, Token.WHERE, Token.JOIN, Token.COMMA);
                    break;
                case SELECT:
                    parseSelect(lexer);
                    break;
                default:
                    break;
            }
            //如果发现时where，则跳出
            if (lexer.token() == Token.WHERE || lexer.token() == Token.SET) {
                break;
            }
        }
    }

    /**
     * 增加子sql
     */
    private void splitSubSql(Lexer lexer) {
        if (lexer.isEOF() && lexerPos > 0) {
            subSqls.add(sql.substring(lexerPos));
        } else {
            subSqls.add(sql.substring(lexerPos, lexer.currentMark()));
        }
        lexerPos = lexer.currentPos();
    }


    /**
     * 设置LexerPos
     */
    private void setLexerPos() {
        lexerPos = lexer.currentPos();
    }

    /**
     * 计算路由。
     * hint路由拥有最高优先级。
     * schema的默认路由为最后保障。
     */
    private void calculateRouteInfo() {

        //检查有没有hint，hint是强行匹配的
        if (hintRouteInfo != null) {
            //此处强行指定路由。
            if ("*".equalsIgnoreCase(hintRouteInfo)) {
                // 匹配全部路由
                if (mainRouteData.tableConfig.getRoute() == null) {
                    this.parseResult.setErrorInfo(ErrorCode.ERR_NO_ROUTE_INFO, "NO TABLE ROUTE INFO: " + sql);
                    return;
                }
                List<RouteAlgorithm.RouteInfo> list = null;
                try {
                    list = RouteManager.getAllRouteList(mainRouteData.tableConfig);
                } catch (RouteAlgorithm.RouteException e) {
                    this.parseResult.setErrorInfo(ErrorCode.ERR_NO_ROUTE_INFO, "NO TABLE ROUTE INFO: " + sql);
                    return;
                }
                RouteAlgorithm.RouteInfoData routeInfoData = new RouteAlgorithm.RouteInfoData();
                routeInfoData.setAll(new HashSet<>(list));
                mainRouteData.routeInfoData = routeInfoData;
            } else {
                //指定路由列表
                HashSet<RouteAlgorithm.RouteInfo> list = new HashSet<>();
                String[] routes = hintRouteInfo.split(",");
                for (String route : routes) {
                    String[] rs = route.split("\\.");
                    if (rs.length == 2) {
                        list.add(new RouteAlgorithm.RouteInfo(rs[0], rs[1], null));
                    }
                }
                RouteAlgorithm.RouteInfoData routeInfoData = new RouteAlgorithm.RouteInfoData();
                routeInfoData.setAll(list);
                mainRouteData.routeInfoData = routeInfoData;
            }
        } else {

            if (mainRouteData == null || mainRouteData.tableConfig == null) {
                //这种情况一般是完全无法匹配的，生成sql的时候直接给默认schema。
                return;
            }
            //外部強行賦值的，直接返回。
            if (mainRouteData.routeInfoData != null) {
                return;
            }

            if (checkTableHasRoute()) {
                //此时Table是有Route的
                if (!mainRouteData.routeKeyData.isEmptyValue()) {
                    //此时说明是sharding配置表。

                    try {
                        mainRouteData.routeInfoData = RouteManager.calculate(mainRouteData.tableConfig, mainRouteData.routeKeyData);
                    } catch (RouteAlgorithm.RouteException e) {
                        this.parseResult.setErrorInfo(ErrorCode.ERR_ROUTE_CALC, "ROUTE CALC ERROR: " + e.getMessage() + ", SQL: " + sql);
                        return;
                    }
                } else {
                    //在路由名单里的，不指定参数，根据匹配类型确定转发。
                    switch (mainRouteData.tableConfig.getMatchType()) {
                        case MATCH_DEFAULT:
                            //此时是非sharding配置表，给schema默认数据。
                            RouteAlgorithm.RouteInfoData routeInfoData = new RouteAlgorithm.RouteInfoData();
                            routeInfoData.setSingle(new RouteAlgorithm.RouteInfo(schema.getBaseNode(), schema.getName(), mainRouteData.tableConfig.getName()));
                            mainRouteData.routeInfoData = routeInfoData;
                            break;
                        case MATCH_ALL:
                            //匹配全部路由
                            RouteAlgorithm.RouteInfoData routeInfoData2 = new RouteAlgorithm.RouteInfoData();
                            try {
                                routeInfoData2.setAll(new HashSet<>(RouteManager.getAllRouteList(mainRouteData.tableConfig)));
                            } catch (RouteAlgorithm.RouteException e) {
                                this.parseResult.setErrorInfo(ErrorCode.ERR_NO_ROUTE_INFO, "NO TABLE ROUTE INFO: " + sql);
                                return;
                            }
                            mainRouteData.routeInfoData = routeInfoData2;
                            break;
                        default:
                            //直接报错吧。
                            this.parseResult.setErrorInfo(ErrorCode.ERR_NO_ROUTE_KEY, "NO ROUTE KEY[ " + mainRouteData.routeKeyData.keyString() + "]:" + sql);
                            return;
                    }
                }
            } else {
                //不在路由名单里的，匹配虚拟schema。
                RouteAlgorithm.RouteInfoData routeInfoData = new RouteAlgorithm.RouteInfoData();
                String schemaName = mainRouteData.schemaName;
                if (schemaName == null) {
                    schemaName = schema.getName();
                } else {
                    //排除系统表。
                    if (!schemaName.equalsIgnoreCase("information_schema") && !schemaName.equalsIgnoreCase("performance_schema") && !schemaName.equalsIgnoreCase("mysql") && !schemaName.equalsIgnoreCase("sys")) {
                        schemaName = schema.getName();
                    }
                }
                routeInfoData.setSingle(new RouteAlgorithm.RouteInfo(schema.getBaseNode(), schemaName, mainRouteData.tableConfig.getName()));
                mainRouteData.routeInfoData = routeInfoData;
            }

            //匹配从表数据
            if (routeDataMap != null) {
                for (RouteData routeData : routeDataMap.values()) {
                    if (routeData.routeInfoData == null) {
                        if (routeData.routeKeyData != null) {
                            try {
                                routeData.routeInfoData = RouteManager.calculate(routeData.tableConfig, routeData.routeKeyData);
                            } catch (RouteAlgorithm.RouteException e) {
                                this.parseResult.setErrorInfo(ErrorCode.ERR_ROUTE_CALC, "ROUTE CALC ERROR: " + e.getMessage() + ", SQL: " + sql);
                                return;
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 根据路由情况，分批合并sql。
     */
    private void generateSqlInfo() {
        //没有匹配到表名，直接给默认schema了。
        if (subSqls.size() <= 1) {
            sqlInfo = new SqlParseResult.SqlInfo(sql);
            sqlInfo.setMysqlGroup(schema.getBaseNode());
            sqlInfo.setDatabase(schema.getName());
            this.parseResult.setSqlInfo(sqlInfo);
            return;
        }
        //每个mainRouteInfoData对应一个mysqlGroup
        if (checkSingleRoute()) {
            sqlInfo = new SqlParseResult.SqlInfo(sql.length() + 32);
            //开始循环加表名
            for (int i = 0; i < subSqls.size(); i++) {
                sqlInfo.appendSql(subSqls.get(i));
                if (i == 0) {
                    //把主表路由加上。
                    RouteAlgorithm.RouteInfo ri = mainRouteData.routeInfoData.getRouteInfo();
                    if (ri == null) {
                        //FIXME axeon@2018/6/16  此处有异常，应妥善处理。
                    }
                    sqlInfo.appendSql(ri.getDatabase()).appendSql(".").appendSql(ri.getTable());
                    sqlInfo.setMysqlGroup(ri.getMysqlGroup());
                    sqlInfo.setDatabase(ri.getDatabase());
                    sqlInfo.setTable(ri.getTable());
                } else if (i < subSqls.size() - 1) {
                    //开始处理从表路由。
                    if (tableList != null) {
                        RouteAlgorithm.RouteInfoData rid = routeDataMap.get(tableList.get(i - 1)).routeInfoData;
                        if (rid != null) {
                            RouteAlgorithm.RouteInfo ri = rid.getRouteInfo();
                            sqlInfo.appendSql(ri.checkValid() ? ri.getDatabase() : sqlInfo.getDatabase()).appendSql(".").appendSql(ri.getTable());
                        }
                    }
                }
            }
            this.parseResult.setSqlInfo(sqlInfo);
        } else {
            sqlInfos = new ArrayList<>();
            SqlParseResult.SqlInfo sb = new SqlParseResult.SqlInfo(sql.length() + 32);
            sqlInfos.add(sb);
            //开始循环加表名
            for (int i = 0; i < subSqls.size(); i++) {
                for (SqlParseResult.SqlInfo si : sqlInfos) {
                    si.appendSql(subSqls.get(i));
                }
                if (i == 0) {
                    //把主表路由加上。
                    appendRouteInfoData(true, mainRouteData.routeInfoData);
                } else if (i < subSqls.size() - 1) {
                    //开始处理从表路由。
                    if (tableList != null) {
                        RouteAlgorithm.RouteInfoData rid = routeDataMap.get(tableList.get(i - 1)).routeInfoData;
                        if (rid != null) {
                            appendRouteInfoData(false, rid);
                        }
                    }
                }
            }
        }
        this.parseResult.setSqlInfos(sqlInfos);

    }

    /**
     * 是否为单一路由？
     *
     * @return
     */
    private boolean checkSingleRoute() {
        boolean isSingle = mainRouteData == null || mainRouteData.isSingle();
        if (routeDataMap != null) {
            for (RouteData routeData : routeDataMap.values()) {
                isSingle = isSingle && routeData.isSingle();
            }
        }
        this.parseResult.setSingle(isSingle);
        return isSingle;
    }

    /**
     * 附加路由信息数据。
     *
     * @param rid
     */
    private void appendRouteInfoData(boolean isMain, RouteAlgorithm.RouteInfoData rid) {
        if (rid.isSingle()) {
            //匹配单个结果。
            RouteAlgorithm.RouteInfo ri = rid.getRouteInfo();
            for (SqlParseResult.SqlInfo si : sqlInfos) {
                si.appendSql(ri.checkValid() ? ri.getDatabase() : si.getDatabase()).appendSql(".").appendSql(ri.getTable());
                if (isMain) {
                    si.setMysqlGroup(ri.getMysqlGroup());
                    si.setDatabase(ri.getDatabase());
                    si.setTable(ri.getTable());
                }
            }
        } else {
            ArrayList<SqlParseResult.SqlInfo> sbxs = new ArrayList<>();
            for (RouteAlgorithm.RouteInfo ri : rid.getRouteInfos()) {
                //此处应该复制多个sql了。。。
                for (SqlParseResult.SqlInfo si : sqlInfos) {
                    SqlParseResult.SqlInfo sqlInfo1 = new SqlParseResult.SqlInfo(sql.length() + 32);
                    sqlInfo1.appendSql(si.getNewSql());
                    sqlInfo1.setMysqlGroup(si.getMysqlGroup());
                    sqlInfo1.setDatabase(si.getDatabase());
                    sqlInfo1.setTable(si.getTable());
                    sqlInfo1.appendSql(ri.checkValid() ? ri.getDatabase() : sqlInfo1.getDatabase()).appendSql(".").appendSql(ri.getTable());
                    if (isMain) {
                        sqlInfo1.setMysqlGroup(ri.getMysqlGroup());
                        sqlInfo1.setDatabase(ri.getDatabase());
                        sqlInfo1.setTable(ri.getTable());
                    }
                    sbxs.add(sqlInfo1);
                }
            }
            sqlInfos = sbxs;
        }
    }

    /**
     * 获得指定的RouteKeyValue
     *
     * @return
     */
    private RouteAlgorithm.RouteKeyValue getRouteKeyValue(String value1, String value2) {
        RouteAlgorithm.RouteKeyValue routeKeyValue = null;
        RouteData routeData = null;
        String column = null;
        //如果没有第二个值。
        if (value2 != null) {
            routeData = getRouteData(value1);
            column = value2;
        } else {
            column = value1;
            routeData = mainRouteData;
        }
        if (routeData != null && routeData.routeKeyData != null) {
            routeKeyValue = routeData.routeKeyData.getValue(column);
        }
        return routeKeyValue;
    }

    private static class RouteData {

        /**
         * 原始的schema信息。
         */
        String schemaName;

        /**
         * 表信息。
         */
        MydbConfig.TableConfig tableConfig;

        /**
         * 绑定的routeKeyData
         */
        RouteAlgorithm.RouteKeyData routeKeyData;

        /**
         * 绑定的routeInfoData
         */
        RouteAlgorithm.RouteInfoData routeInfoData;

        /**
         * 是否单一路由
         *
         * @return
         */
        public boolean isSingle() {
            if (routeInfoData != null) {
                return routeInfoData.isSingle();
            } else {
                return true;
            }
        }
    }

}
