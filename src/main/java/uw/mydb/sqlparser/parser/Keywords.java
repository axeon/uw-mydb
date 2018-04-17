package uw.mydb.sqlparser.parser;


import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 关键字列表。 基于druid的修改版本。
 *
 * @author wenshao [szujobs@hotmail.com]
 */
public class Keywords {

    public final static Keywords DEFAULT_KEYWORDS;

    static {
        Map<String, Token> map = new HashMap<String, Token>();

        map.put("ALL", Token.ALL);
        map.put("ALTER", Token.ALTER);
        map.put("AND", Token.AND);
        map.put("ANY", Token.ANY);
        map.put("AS", Token.AS);

        map.put("ENABLE", Token.ENABLE);
        map.put("DISABLE", Token.DISABLE);

        map.put("ASC", Token.ASC);
        map.put("BETWEEN", Token.BETWEEN);
        map.put("BY", Token.BY);
        map.put("CASE", Token.CASE);
        map.put("CAST", Token.CAST);

        map.put("CHECK", Token.CHECK);
        map.put("CONSTRAINT", Token.CONSTRAINT);
        map.put("CREATE", Token.CREATE);
        map.put("DATABASE", Token.DATABASE);
        map.put("DEFAULT", Token.DEFAULT);
        map.put("COLUMN", Token.COLUMN);
        map.put("TABLESPACE", Token.TABLESPACE);
        map.put("PROCEDURE", Token.PROCEDURE);
        map.put("FUNCTION", Token.FUNCTION);

        map.put("DELETE", Token.DELETE);
        map.put("DESC", Token.DESC);
        map.put("DESCRIBE", Token.DESCRIBE);
        map.put("DISTINCT", Token.DISTINCT);
        map.put("DROP", Token.DROP);
        map.put("ELSE", Token.ELSE);
        map.put("EXPLAIN", Token.EXPLAIN);
        map.put("EXCEPT", Token.EXCEPT);

        map.put("END", Token.END);
        map.put("ESCAPE", Token.ESCAPE);
        map.put("EXISTS", Token.EXISTS);
        map.put("FOR", Token.FOR);
        map.put("FOREIGN", Token.FOREIGN);

        map.put("FROM", Token.FROM);
        map.put("FULL", Token.FULL);
        map.put("GROUP", Token.GROUP);
        map.put("HAVING", Token.HAVING);
        map.put("IN", Token.IN);

        map.put("INDEX", Token.INDEX);
        map.put("INNER", Token.INNER);
        map.put("INSERT", Token.INSERT);
        map.put("INTERSECT", Token.INTERSECT);
        map.put("INTERVAL", Token.INTERVAL);

        map.put("INTO", Token.INTO);
        map.put("IS", Token.IS);
        map.put("JOIN", Token.JOIN);
        map.put("KEY", Token.KEY);
        map.put("LEFT", Token.LEFT);

        map.put("LIKE", Token.LIKE);
        map.put("LOCK", Token.LOCK);
        map.put("MINUS", Token.MINUS);
        map.put("NOT", Token.NOT);

        map.put("NULL", Token.NULL);
        map.put("ON", Token.ON);
        map.put("OR", Token.OR);
        map.put("ORDER", Token.ORDER);
        map.put("OUTER", Token.OUTER);

        map.put("PRIMARY", Token.PRIMARY);
        map.put("REFERENCES", Token.REFERENCES);
        map.put("RIGHT", Token.RIGHT);
        map.put("SCHEMA", Token.SCHEMA);
        map.put("SELECT", Token.SELECT);

        map.put("SET", Token.SET);
        map.put("SOME", Token.SOME);
        map.put("TABLE", Token.TABLE);
        map.put("THEN", Token.THEN);
        map.put("TRUNCATE", Token.TRUNCATE);

        map.put("UNION", Token.UNION);
        map.put("UNIQUE", Token.UNIQUE);
        map.put("UPDATE", Token.UPDATE);
        map.put("VALUES", Token.VALUES);
        map.put("VIEW", Token.VIEW);
        map.put("SEQUENCE", Token.SEQUENCE);
        map.put("TRIGGER", Token.TRIGGER);
        map.put("USER", Token.USER);

        map.put("WHEN", Token.WHEN);
        map.put("WHERE", Token.WHERE);
        map.put("XOR", Token.XOR);

        map.put("OVER", Token.OVER);
        map.put("TO", Token.TO);
        map.put("USE", Token.USE);

        map.put("REPLACE", Token.REPLACE);

        map.put("COMMENT", Token.COMMENT);
        map.put("COMPUTE", Token.COMPUTE);
        map.put("WITH", Token.WITH);
        map.put("GRANT", Token.GRANT);
        map.put("REVOKE", Token.REVOKE);

        // MySql procedure: add by zz
        map.put("WHILE", Token.WHILE);
        map.put("DO", Token.DO);
        map.put("DECLARE", Token.DECLARE);
        map.put("LOOP", Token.LOOP);
        map.put("LEAVE", Token.LEAVE);
        map.put("ITERATE", Token.ITERATE);
        map.put("REPEAT", Token.REPEAT);
        map.put("UNTIL", Token.UNTIL);
        map.put("OPEN", Token.OPEN);
        map.put("CLOSE", Token.CLOSE);
        map.put("CURSOR", Token.CURSOR);
        map.put("FETCH", Token.FETCH);
        map.put("OUT", Token.OUT);
        map.put("INOUT", Token.INOUT);

        map.put("DUAL", Token.DUAL);
        map.put("FALSE", Token.FALSE);
        map.put("IDENTIFIED", Token.IDENTIFIED);
        map.put("IF", Token.IF);
        map.put("KILL", Token.KILL);

        map.put("LIMIT", Token.LIMIT);
        map.put("TRUE", Token.TRUE);
        map.put("BINARY", Token.BINARY);
        map.put("SHOW", Token.SHOW);
        map.put("CACHE", Token.CACHE);
        map.put("ANALYZE", Token.ANALYZE);
        map.put("OPTIMIZE", Token.OPTIMIZE);
        map.put("ROW", Token.ROW);
        map.put("BEGIN", Token.BEGIN);
        map.put("END", Token.END);
        map.put("DIV", Token.DIV);
        map.put("MERGE", Token.MERGE);

        // for oceanbase & mysql 5.7
        map.put("PARTITION", Token.PARTITION);

        map.put("CONTINUE", Token.CONTINUE);
        map.put("UNDO", Token.UNDO);
        map.put("SQLSTATE", Token.SQLSTATE);
        map.put("CONDITION", Token.CONDITION);

        DEFAULT_KEYWORDS = new Keywords(map);

    }

    private final Map<String, Token> keywords;
    private long[] hashArray;
    private Token[] tokens;

    public Keywords(Map<String, Token> keywords) {
        this.keywords = keywords;

        this.hashArray = new long[keywords.size()];
        this.tokens = new Token[keywords.size()];

        int index = 0;
        for (String k : keywords.keySet()) {
            hashArray[index++] = FnvHash.fnv1a_64_lower(k);
        }
        Arrays.sort(hashArray);
        for (Map.Entry<String, Token> entry : keywords.entrySet()) {
            long k = FnvHash.fnv1a_64_lower(entry.getKey());
            index = Arrays.binarySearch(hashArray, k);
            tokens[index] = entry.getValue();
        }
    }

    public boolean containsValue(Token token) {
        return this.keywords.containsValue(token);
    }

    public Token getKeyword(long hash) {
        int index = Arrays.binarySearch(hashArray, hash);
        if (index < 0) {
            return null;
        }
        return tokens[index];
    }

    public Token getKeyword(String key) {
        long k = FnvHash.fnv1a_64_lower(key);
        int index = Arrays.binarySearch(hashArray, k);
        if (index < 0) {
            return null;
        }
        return tokens[index];
//        return keywords.get(key);
    }

    public Map<String, Token> getKeywords() {
        return keywords;
    }

}
