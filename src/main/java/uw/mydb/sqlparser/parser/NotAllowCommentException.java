package uw.mydb.sqlparser.parser;

/**
 * 基于druid的修改版本。
 *
 * @author axeon
 */
public class NotAllowCommentException extends ParserException {

    private static final long serialVersionUID = 1L;

    public NotAllowCommentException() {
        this("comment not allow");
    }

    public NotAllowCommentException(String message, Throwable e) {
        super(message, e);
    }

    public NotAllowCommentException(String message) {
        super(message);
    }

}
