package uw.mydb.sqlparser.parser;

import java.io.Serializable;

/**
 * 基于druid的修改版本。
 *
 * @author axeon
 */
public class ParserException extends RuntimeException implements Serializable {

    private static final long serialVersionUID = 1L;

    public ParserException() {
    }

    public ParserException(String message) {
        super(message);
    }

    public ParserException(String message, Throwable e) {
        super(message, e);
    }

    public ParserException(String message, int line, int col) {
        super(message);
    }

    public ParserException(Throwable ex, String ksql) {
        super("parse error. detail message is :\n" + ex.getMessage() + "\nsource sql is : \n" + ksql, ex);
    }
}
