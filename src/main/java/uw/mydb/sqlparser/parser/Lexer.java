package uw.mydb.sqlparser.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static uw.mydb.sqlparser.parser.CharTypes.*;
import static uw.mydb.sqlparser.parser.LayoutCharacters.EOI;
import static uw.mydb.sqlparser.parser.Token.*;

/**
 * 来源于druid的sql解析器，在此基础上进行了修改，保留原有授权信息和作者信息。
 *
 * @author wenshao [szujobs@hotmail.com]
 * @author axeon [23231269@qq.com]
 */
public class Lexer {

    /**
     * 解析的文本字符串。
     */
    private final String text;

    /**
     * 当前位置
     */
    private int pos;

    /**
     * 标记位置
     */
    private int mark;

    /**
     * 当前char
     */
    private char ch;
    private char[] buf;
    private int bufPos;

    /**
     * 当前解析的token。
     */
    private Token token;

    /**
     * keywords
     */
    private Keywords keywords = Keywords.DEFAULT_KEYWORDS;

    /**
     * 当前解析的字符串。
     */
    private String stringVal;
    private long hash_lower; // fnv1a_64
    private long hash;
    private int commentCount = 0;
    private List<String> comments = null;
    private boolean skipComment = true;
    private CommentHandler commentHandler;
    private boolean endOfComment = false;
    private boolean keepComments = true;
    private boolean optimizedForParameterized = false;
    /**
     * 保存检查点。
     */
    private SavePoint savePoint = null;

    public Lexer(String input) {
        this(input, false, true);
    }

    public Lexer(String input, boolean skipComment, boolean keepComments) {
        this.skipComment = skipComment;
        this.keepComments = keepComments;
        this.text = input;
        this.pos = 0;
        ch = charAt(pos);
    }

    /**
     * 参数化sql。
     *
     * @param sql
     * @return
     */
    public static String parameterize(String sql) {
        Lexer lexer = new Lexer(sql);

        lexer.optimizedForParameterized = true; // optimized

        lexer.nextToken();

        StringBuffer buf = new StringBuffer();

        for_:
        for (; ; ) {
            Token token = lexer.token;
            switch (token) {
                case LITERAL_ALIAS:
                case LITERAL_FLOAT:
                case LITERAL_CHARS:
                case LITERAL_INT:
                case LITERAL_NCHARS:
                case LITERAL_HEX:
                case VARIANT:
                    if (buf.length() != 0) {
                        buf.append(' ');
                    }
                    buf.append('?');
                    break;
                case COMMA:
                    buf.append(',');
                    break;
                case EQ:
                    buf.append('=');
                    break;
                case EOF:
                    break for_;
                case ERROR:
                    return sql;
                case SELECT:
                    buf.append("SELECT");
                    break;
                case UPDATE:
                    buf.append("UPDATE");
                    break;
                default:
                    if (buf.length() != 0) {
                        buf.append(' ');
                    }
                    lexer.stringVal(buf);
                    break;
            }

            lexer.nextToken();
        }

        return buf.toString();
    }

    public CommentHandler getCommentHandler() {
        return commentHandler;
    }

    public void setCommentHandler(CommentHandler commentHandler) {
        this.commentHandler = commentHandler;
    }

    private final char charAt(int index) {
        if (index >= text.length()) {
            return EOI;
        }
        return text.charAt(index);
    }

    private final String addSymbol() {
        return subString(mark, bufPos);
    }

    private final String subString(int offset, int count) {
        return text.substring(offset, offset + count);
    }

    private final char[] subChars(int offset, int count) {
        char[] chars = new char[count];
        text.getChars(offset, offset + count, chars, 0);
        return chars;
    }

    private void arraycopy(int srcPos, char[] dest, int destPos, int length) {
        text.getChars(srcPos, srcPos + length, dest, destPos);
    }

    /**
     * 初始化指定大小的buf。
     *
     * @param size
     */
    private void initBuff(int size) {
        if (buf == null) {
            if (size < 32) {
                buf = new char[32];
            } else {
                buf = new char[size + 32];
            }
        } else if (buf.length < size) {
            buf = Arrays.copyOf(buf, size);
        }
    }

    /**
     * 标记解析位置。
     *
     * @return
     */
    public SavePoint mark() {
        SavePoint savePoint = new SavePoint();
        savePoint.bp = pos;
        savePoint.sp = bufPos;
        savePoint.np = mark;
        savePoint.ch = ch;
        savePoint.token = token;
        savePoint.stringVal = stringVal;
        savePoint.hash = hash;
        savePoint.hash_lower = hash_lower;
        return this.savePoint = savePoint;
    }

    /**
     * 恢复解析位置。
     *
     * @param savePoint
     */
    public void reset(SavePoint savePoint) {
        this.pos = savePoint.bp;
        this.bufPos = savePoint.sp;
        this.mark = savePoint.np;
        this.ch = savePoint.ch;
        this.token = savePoint.token;
        this.stringVal = savePoint.stringVal;
        this.hash = savePoint.hash;
        this.hash_lower = savePoint.hash_lower;
    }

    /**
     * 恢复解析位置。
     */
    public void reset() {
        this.reset(this.savePoint);
    }

    /**
     * 重置位置。
     *
     * @param pos
     */
    public void reset(int pos) {
        this.pos = pos;
        this.ch = charAt(pos);
    }

    /**
     * 前向扫描字符。
     */
    protected final void scanChar() {
        ch = charAt(++pos);
    }

    /**
     * 回退扫描字符。
     */
    protected void unscan() {
        ch = charAt(--pos);
    }

    /**
     * 是否已结束。
     *
     * @return
     */
    public boolean isEOF() {
        return pos >= text.length();
    }

    /**
     * Report an error at the given position using the provided arguments.
     */
    protected void lexError(String key, Object... args) {
        token = ERROR;
    }

    /**
     * 返回当前token。
     */
    public final Token token() {
        return token;
    }

    /**
     * 当前解析位置信息。
     *
     * @return
     */
    public String info() {
        StringBuilder buf = new StringBuilder();
        buf.append("pos ").append(pos).append(", token ").append(token);
        if (token == Token.IDENTIFIER || token == Token.LITERAL_ALIAS || token == Token.LITERAL_CHARS) {
            buf.append(" ").append(stringVal);
        }
        return buf.toString();
    }

    /**
     * 寻找下一个comma。
     */
    public final void nextTokenComma() {
        if (ch == ' ') {
            scanChar();
        }

        if (ch == ',' || ch == '，') {
            scanChar();
            token = COMMA;
            return;
        }

        if (ch == ')' || ch == '）') {
            scanChar();
            token = RPAREN;
            return;
        }

        if (ch == '.') {
            scanChar();
            token = DOT;
            return;
        }

        if (ch == 'a' || ch == 'A') {
            char ch_next = charAt(pos + 1);
            if (ch_next == 's' || ch_next == 'S') {
                char ch_next_2 = charAt(pos + 2);
                if (ch_next_2 == ' ') {
                    pos += 2;
                    ch = ' ';
                    token = Token.AS;
                    stringVal = "AS";
                    return;
                }
            }
        }

        nextToken();
    }

    public final void nextTokenEq() {
        if (ch == ' ') {
            scanChar();
        }

        if (ch == '=') {
            scanChar();
            token = EQ;
            return;
        }

        if (ch == '.') {
            scanChar();
            token = DOT;
            return;
        }

        if (ch == 'a' || ch == 'A') {
            char ch_next = charAt(pos + 1);
            if (ch_next == 's' || ch_next == 'S') {
                char ch_next_2 = charAt(pos + 2);
                if (ch_next_2 == ' ') {
                    pos += 2;
                    ch = ' ';
                    token = Token.AS;
                    stringVal = "AS";
                    return;
                }
            }
        }

        nextToken();
    }

    public final void nextTokenLParen() {
        if (ch == ' ') {
            scanChar();
        }

        if (ch == '(' || ch == '（') {
            scanChar();
            token = LPAREN;
            return;
        }
        nextToken();
    }

    public final void nextTokenValue() {
        if (ch == ' ') {
            scanChar();
        }

        if (ch == '\'') {
            bufPos = 0;
            scanString();
            return;
        }

        if (ch >= '0' && ch <= '9') {
            bufPos = 0;
            scanNumber();
            return;
        }

        if (ch == '?') {
            scanChar();
            token = Token.QUES;
            return;
        }

        if (ch == 'n' || ch == 'N') {
            char c1 = 0, c2, c3, c4;
            if (pos + 4 < text.length()
                    && ((c1 = text.charAt(pos + 1)) == 'u' || c1 == 'U')
                    && ((c2 = text.charAt(pos + 2)) == 'l' || c2 == 'L')
                    && ((c3 = text.charAt(pos + 3)) == 'l' || c3 == 'L')
                    && (isWhitespace(c4 = text.charAt(pos + 4)) || c4 == ',' || c4 == ')')) {
                pos += 4;
                ch = c4;
                token = Token.NULL;
                stringVal = "NULL";
                return;
            }

            if (c1 == '\'') {
                ++pos;
                ch = '\'';
                scanString();
                token = Token.LITERAL_NCHARS;
                return;
            }
        }

        if (ch == ')') {
            scanChar();
            token = Token.RPAREN;
            return;
        }

        if (isFirstIdentifierChar(ch)) {
            scanIdentifier();
            return;
        }

        nextToken();
    }

    public final void nextTokenBy() {
        while (ch == ' ') {
            scanChar();
        }

        if (ch == 'b' || ch == 'B') {
            char ch_next = charAt(pos + 1);
            if (ch_next == 'y' || ch_next == 'Y') {
                char ch_next_2 = charAt(pos + 2);
                if (ch_next_2 == ' ') {
                    pos += 2;
                    ch = ' ';
                    token = Token.BY;
                    stringVal = "BY";
                    return;
                }
            }
        }

        nextToken();
    }

    public final void nextTokenNotOrNull() {
        while (ch == ' ') {
            scanChar();
        }


        if ((ch == 'n' || ch == 'N') && pos + 3 < text.length()) {
            char c1 = text.charAt(pos + 1);
            char c2 = text.charAt(pos + 2);
            char c3 = text.charAt(pos + 3);

            if ((c1 == 'o' || c1 == 'O')
                    && (c2 == 't' || c2 == 'T')
                    && isWhitespace(c3)) {
                pos += 3;
                ch = c3;
                token = Token.NOT;
                stringVal = "NOT";
                return;
            }

            char c4;
            if (pos + 4 < text.length()
                    && (c1 == 'u' || c1 == 'U')
                    && (c2 == 'l' || c2 == 'L')
                    && (c3 == 'l' || c3 == 'L')
                    && isWhitespace(c4 = text.charAt(pos + 4))) {
                pos += 4;
                ch = c4;
                token = Token.NULL;
                stringVal = "NULL";
                return;
            }
        }

        nextToken();
    }

    public final void nextTokenIdent() {
        while (ch == ' ') {
            scanChar();
        }

        if (isFirstIdentifierChar(ch)) {
            scanIdentifier();
            return;
        }

        if (ch == ')') {
            scanChar();
            token = RPAREN;
            return;
        }

        nextToken();
    }

    /**
     * 跳到下一个token。
     */
    public final void nextToken() {
        bufPos = 0;
        if (comments != null && comments.size() > 0) {
            comments = null;
        }

        for (; ; ) {
            //空白跳过
            if (isWhitespace(ch)) {
                ch = charAt(++pos);
                continue;
            }

            //变量
            if (ch == '$' && charAt(pos + 1) == '{') {
                scanVariable();
                return;
            }


            if (isFirstIdentifierChar(ch)) {
                if (ch == '（') {
                    scanChar();
                    token = LPAREN;
                    return;
                } else if (ch == '）') {
                    scanChar();
                    token = RPAREN;
                    return;
                }

                if (ch == 'N' || ch == 'n') {
                    if (charAt(pos + 1) == '\'') {
                        ++pos;
                        ch = '\'';
                        scanString();
                        token = Token.LITERAL_NCHARS;
                        return;
                    }
                }

                scanIdentifier();
                return;
            }

            switch (ch) {
                case '0':
                    if (charAt(pos + 1) == 'x') {
                        scanChar();
                        scanChar();
                        scanHexaDecimal();
                    } else {
                        scanNumber();
                    }
                    return;
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    scanNumber();
                    return;
                case ',':
                case '，':
                    scanChar();
                    token = COMMA;
                    return;
                case '(':
                case '（':
                    scanChar();
                    token = LPAREN;
                    return;
                case ')':
                case '）':
                    scanChar();
                    token = RPAREN;
                    return;
                case '[':
                    scanLBracket();
                    return;
                case ']':
                    scanChar();
                    token = RBRACKET;
                    return;
                case '{':
                    scanChar();
                    token = LBRACE;
                    return;
                case '}':
                    scanChar();
                    token = RBRACE;
                    return;
                case ':':
                    scanChar();
                    if (ch == '=') {
                        scanChar();
                        token = COLONEQ;
                    } else if (ch == ':') {
                        scanChar();
                        token = COLONCOLON;
                    } else {
                        unscan();
                        scanVariable();
                    }
                    return;
                case '#':
                    scanSharp();
                    if ((token == Token.LINE_COMMENT || token == Token.MULTI_LINE_COMMENT) && skipComment) {
                        bufPos = 0;
                        continue;
                    }
                    return;
                case '.':
                    scanChar();
                    if (isDigit(ch) && !isFirstIdentifierChar(charAt(pos - 2))) {
                        unscan();
                        scanNumber();
                        return;
                    } else if (ch == '.') {
                        scanChar();
                        if (ch == '.') {
                            scanChar();
                            token = Token.DOTDOTDOT;
                        } else {
                            token = Token.DOTDOT;
                        }
                    } else {
                        token = Token.DOT;
                    }
                    return;
                case '\'':
                    scanString();
                    return;
                case '\"':
                    scanAlias();
                    return;
                case '*':
                    scanChar();
                    token = Token.STAR;
                    return;
                case '?':
                    scanChar();

                    token = Token.QUES;

                    return;
                case ';':
                    scanChar();
                    token = Token.SEMI;
                    return;
                case '`':
                    throw new ParserException("TODO. " + info()); // TODO
                case '@':
                    scanVariable_at();
                    return;
                case '-':
                    if (charAt(pos + 1) == '-') {
                        scanComment();
                        if ((token == Token.LINE_COMMENT || token == Token.MULTI_LINE_COMMENT) && skipComment) {
                            bufPos = 0;
                            continue;
                        }
                    } else {
                        scanOperator();
                    }
                    return;
                case '/':
                    int nextChar = charAt(pos + 1);
                    if (nextChar == '/' || nextChar == '*') {
                        scanComment();
                        if ((token == Token.LINE_COMMENT || token == Token.MULTI_LINE_COMMENT) && skipComment) {
                            bufPos = 0;
                            continue;
                        }
                    } else {
                        token = Token.SLASH;
                        scanChar();
                    }
                    return;
                default:
                    if (Character.isLetter(ch)) {
                        scanIdentifier();
                        return;
                    }

                    if (isOperator(ch)) {
                        scanOperator();
                        return;
                    }

                    if (ch == '\\' && charAt(pos + 1) == 'N') {
                        scanChar();
                        scanChar();
                        token = Token.NULL;
                        return;
                    }

                    // QS_TODO ?
                    if (isEOF()) { // JLS
                        token = EOF;
                    } else {
                        lexError("illegal.char", String.valueOf((int) ch));
                        scanChar();
                    }

                    return;
            }
        }

    }

    protected void scanLBracket() {
        scanChar();
        token = LBRACKET;
    }

    private final void scanOperator() {
        switch (ch) {
            case '+':
                scanChar();
                token = Token.PLUS;
                break;
            case '-':
                scanChar();
                if (ch == '>') {
                    scanChar();
                    if (ch == '>') {
                        scanChar();
                        token = Token.SUBGTGT;
                    } else {
                        token = Token.SUBGT;
                    }
                } else {
                    token = Token.SUB;
                }
                break;
            case '*':
                scanChar();
                token = Token.STAR;
                break;
            case '/':
                scanChar();
                token = Token.SLASH;
                break;
            case '&':
                scanChar();
                if (ch == '&') {
                    scanChar();
                    token = Token.AMPAMP;
                } else {
                    token = Token.AMP;
                }
                break;
            case '|':
                scanChar();
                if (ch == '|') {
                    scanChar();
                    if (ch == '/') {
                        scanChar();
                        token = Token.BARBARSLASH;
                    } else {
                        token = Token.BARBAR;
                    }
                } else if (ch == '/') {
                    scanChar();
                    token = Token.BARSLASH;
                } else {
                    token = Token.BAR;
                }
                break;
            case '^':
                scanChar();
                if (ch == '=') {
                    scanChar();
                    token = Token.CARETEQ;
                } else {
                    token = Token.CARET;
                }
                break;
            case '%':
                scanChar();
                token = Token.PERCENT;
                break;
            case '=':
                scanChar();
                if (ch == '=') {
                    scanChar();
                    token = Token.EQEQ;
                } else if (ch == '>') {
                    scanChar();
                    token = Token.EQGT;
                } else {
                    token = Token.EQ;
                }
                break;
            case '>':
                scanChar();
                if (ch == '=') {
                    scanChar();
                    token = Token.GTEQ;
                } else if (ch == '>') {
                    scanChar();
                    token = Token.GTGT;
                } else {
                    token = Token.GT;
                }
                break;
            case '<':
                scanChar();
                if (ch == '=') {
                    scanChar();
                    if (ch == '>') {
                        token = Token.LTEQGT;
                        scanChar();
                    } else {
                        token = Token.LTEQ;
                    }
                } else if (ch == '>') {
                    scanChar();
                    token = Token.LTGT;
                } else if (ch == '<') {
                    scanChar();
                    token = Token.LTLT;
                } else if (ch == '@') {
                    scanChar();
                    token = Token.LT_MONKEYS_AT;
                } else if (ch == '-' && charAt(pos + 1) == '>') {
                    scanChar();
                    scanChar();
                    token = Token.LT_SUB_GT;
                } else {
                    if (ch == ' ') {
                        char c1 = charAt(pos + 1);
                        if (c1 == '=') {
                            scanChar();
                            scanChar();
                            if (ch == '>') {
                                token = Token.LTEQGT;
                                scanChar();
                            } else {
                                token = Token.LTEQ;
                            }
                        } else if (c1 == '>') {
                            scanChar();
                            scanChar();
                            token = Token.LTGT;
                        } else if (c1 == '<') {
                            scanChar();
                            scanChar();
                            token = Token.LTLT;
                        } else if (c1 == '@') {
                            scanChar();
                            scanChar();
                            token = Token.LT_MONKEYS_AT;
                        } else if (c1 == '-' && charAt(pos + 2) == '>') {
                            scanChar();
                            scanChar();
                            scanChar();
                            token = Token.LT_SUB_GT;
                        } else {
                            token = Token.LT;
                        }
                    } else {
                        token = Token.LT;
                    }
                }
                break;
            case '!':
                scanChar();
                while (isWhitespace(ch)) {
                    scanChar();
                }
                if (ch == '=') {
                    scanChar();
                    token = Token.BANGEQ;
                } else if (ch == '>') {
                    scanChar();
                    token = Token.BANGGT;
                } else if (ch == '<') {
                    scanChar();
                    token = Token.BANGLT;
                } else if (ch == '!') {
                    scanChar();
                    token = Token.BANGBANG; // postsql
                } else if (ch == '~') {
                    scanChar();
                    if (ch == '*') {
                        scanChar();
                        token = Token.BANG_TILDE_STAR; // postsql
                    } else {
                        token = Token.BANG_TILDE; // postsql
                    }
                } else {
                    token = Token.BANG;
                }
                break;
            case '?':
                scanChar();
                token = Token.QUES;
                break;
            case '~':
                scanChar();
                if (ch == '*') {
                    scanChar();
                    token = Token.TILDE_STAR;
                } else if (ch == '=') {
                    scanChar();
                    token = Token.TILDE_EQ; // postsql
                } else {
                    token = Token.TILDE;
                }
                break;
            default:
                throw new ParserException("TODO. " + info());
        }
    }


    protected final void scanString() {
        {
            boolean hasSpecial = false;
            int startIndex = pos + 1;
            int endIndex = -1; // text.indexOf('\'', startIndex);
            for (int i = startIndex; i < text.length(); ++i) {
                final char ch = text.charAt(i);
                if (ch == '\\') {
                    hasSpecial = true;
                    continue;
                }
                if (ch == '\'') {
                    endIndex = i;
                    break;
                }
            }

            if (endIndex == -1) {
                throw new ParserException("unclosed str. " + info());
            }

            String stringVal;
            if (token == Token.AS) {
                stringVal = subString(pos, endIndex + 1 - pos);
            } else {
                stringVal = subString(startIndex, endIndex - startIndex);
            }
            // hasSpecial = stringVal.indexOf('\\') != -1;

            if (!hasSpecial) {
                this.stringVal = stringVal;
                int pos = endIndex + 1;
                char ch = charAt(pos);
                if (ch != '\'') {
                    this.pos = pos;
                    this.ch = ch;
                    token = LITERAL_CHARS;
                    return;
                }
            }
        }

        mark = pos;
        boolean hasSpecial = false;
        for (; ; ) {
            if (isEOF()) {
                lexError("unclosed.str.lit");
                return;
            }

            ch = charAt(++pos);

            if (ch == '\\') {
                scanChar();
                if (!hasSpecial) {
                    initBuff(bufPos);
                    arraycopy(mark + 1, buf, 0, bufPos);
                    hasSpecial = true;
                }

                switch (ch) {
                    case '0':
                        putChar('\0');
                        break;
                    case '\'':
                        putChar('\'');
                        break;
                    case '"':
                        putChar('"');
                        break;
                    case 'b':
                        putChar('\b');
                        break;
                    case 'n':
                        putChar('\n');
                        break;
                    case 'r':
                        putChar('\r');
                        break;
                    case 't':
                        putChar('\t');
                        break;
                    case '\\':
                        putChar('\\');
                        break;
                    case 'Z':
                        putChar((char) 0x1A); // ctrl + Z
                        break;
                    case '%':
                        putChar('\\');
                        putChar(ch);
                        break;
                    default:
                        putChar(ch);
                        break;
                }

                continue;
            }
            if (ch == '\'') {
                scanChar();
                if (ch != '\'') {
                    token = LITERAL_CHARS;
                    break;
                } else {
                    if (!hasSpecial) {
                        initBuff(bufPos);
                        arraycopy(mark + 1, buf, 0, bufPos);
                        hasSpecial = true;
                    }
                    putChar('\'');
                    continue;
                }
            }

            if (!hasSpecial) {
                bufPos++;
                continue;
            }

            if (bufPos == buf.length) {
                putChar(ch);
            } else {
                buf[bufPos++] = ch;
            }
        }

        if (!hasSpecial) {
            stringVal = subString(mark + 1, bufPos);
        } else {
            stringVal = new String(buf, 0, bufPos);
        }
    }

    protected final void scanAlias() {
        {
            boolean hasSpecial = false;
            int startIndex = pos + 1;
            int endIndex = -1; // text.indexOf('\'', startIndex);
            for (int i = startIndex; i < text.length(); ++i) {
                final char ch = text.charAt(i);
                if (ch == '\\') {
                    hasSpecial = true;
                    continue;
                }
                if (ch == '"') {
                    if (i + 1 < text.length()) {
                        char ch_next = charAt(i + 1);
                        if (ch_next == '"' || ch_next == '\'') {
                            hasSpecial = true;
                            i++;
                            continue;
                        }
                    }
                    if (i > 0) {
                        char ch_last = charAt(i - 1);
                        if (ch_last == '\'') {
                            hasSpecial = true;
                            continue;
                        }
                    }
                    endIndex = i;
                    break;
                }
            }

            if (endIndex == -1) {
                throw new ParserException("unclosed str. " + info());
            }

            String stringVal = subString(pos, endIndex + 1 - pos);
            // hasSpecial = stringVal.indexOf('\\') != -1;

            if (!hasSpecial) {
                this.stringVal = stringVal;
                int pos = endIndex + 1;
                char ch = charAt(pos);
                if (ch != '\'') {
                    this.pos = pos;
                    this.ch = ch;
                    token = LITERAL_ALIAS;
                    return;
                }
            }
        }

        mark = pos;
        initBuff(bufPos);
        //putChar(ch);

        for (; ; ) {
            if (isEOF()) {
                lexError("unclosed.str.lit");
                return;
            }

            ch = charAt(++pos);

            if (ch == '\\') {
                scanChar();

                switch (ch) {
                    case '0':
                        putChar('\0');
                        break;
                    case '\'':
                        putChar('\'');
                        break;
                    case '"':
                        putChar('"');
                        break;
                    case 'b':
                        putChar('\b');
                        break;
                    case 'n':
                        putChar('\n');
                        break;
                    case 'r':
                        putChar('\r');
                        break;
                    case 't':
                        putChar('\t');
                        break;
                    case '\\':
                        putChar('\\');
                        break;
                    case 'Z':
                        putChar((char) 0x1A); // ctrl + Z
                        break;
                    default:
                        putChar(ch);
                        break;
                }

                continue;
            }

            if (ch == '\'') {
                char ch_next = charAt(pos + 1);
                if (ch_next == '"') {
                    scanChar();
                    continue;
                }
            } else if (ch == '\"') {
                char ch_next = charAt(pos + 1);
                if (ch_next == '"' || ch_next == '\'') {
                    scanChar();
                    continue;
                }

                //putChar(ch);
                scanChar();
                token = LITERAL_CHARS;
                break;
            }

            if (bufPos == buf.length) {
                putChar(ch);
            } else {
                buf[bufPos++] = ch;
            }
        }

        stringVal = new String(buf, 0, bufPos);
    }


    public void scanSharp() {
        if (ch != '#') {
            throw new ParserException("illegal stat. " + info());
        }

        if (charAt(pos + 1) == '{') {
            scanVariable();
            return;
        }

        Token lastToken = this.token;

        scanChar();
        mark = pos;
        bufPos = 0;
        for (; ; ) {
            if (ch == '\r') {
                if (charAt(pos + 1) == '\n') {
                    bufPos += 2;
                    scanChar();
                    break;
                }
                bufPos++;
                break;
            } else if (ch == EOI) {
                break;
            }

            if (ch == '\n') {
                scanChar();
                bufPos++;
                break;
            }

            scanChar();
            bufPos++;
        }

        stringVal = subString(mark - 1, bufPos + 1);
        token = Token.LINE_COMMENT;
        commentCount++;
        if (keepComments) {
            addComment(stringVal);
        }

        if (commentHandler != null && commentHandler.handle(lastToken, stringVal)) {
            return;
        }

        endOfComment = isEOF();

        if (isEOF() || !isSafeComment(stringVal)) {
            throw new NotAllowCommentException();
        }
    }

    /**
     * 扫描变量
     */
    public void scanVariable() {
        if (ch != ':' && ch != '#' && ch != '$') {
            throw new ParserException("illegal variable. " + info());
        }

        mark = pos;
        bufPos = 1;

        if (charAt(pos + 1) == '`') {
            ++pos;
            ++bufPos;
            char ch;
            for (; ; ) {
                ch = charAt(++pos);

                if (ch == '`') {
                    bufPos++;
                    ch = charAt(++pos);
                    break;
                } else if (ch == EOI) {
                    throw new ParserException("illegal identifier. " + info());
                }

                bufPos++;
                continue;
            }

            this.ch = charAt(pos);
            stringVal = subString(mark, bufPos);
            token = Token.VARIANT;
        } else if (charAt(pos + 1) == '{') {
            ++pos;
            ++bufPos;
            char ch;
            for (; ; ) {
                ch = charAt(++pos);

                if (ch == '}') {
                    bufPos++;
                    ch = charAt(++pos);
                    break;
                } else if (ch == EOI) {
                    throw new ParserException("illegal identifier. " + info());
                }

                bufPos++;
                continue;
            }

            this.ch = charAt(pos);

            stringVal = subString(mark, bufPos);
            token = Token.VARIANT;
        } else {
            for (; ; ) {
                ch = charAt(++pos);

                if (!isIdentifierChar(ch)) {
                    break;
                }

                bufPos++;
                continue;
            }
        }

        this.ch = charAt(pos);

        stringVal = subString(mark, bufPos);
        token = Token.VARIANT;
    }

    /**
     * 扫描@变量
     */
    protected void scanVariable_at() {
        if (ch != '@') {
            throw new ParserException("illegal variable. " + info());
        }

        mark = pos;
        bufPos = 1;

        if (charAt(pos + 1) == '@') {
            ch = charAt(++pos);
            bufPos++;
        }

        if (charAt(pos + 1) == '`') {
            ++pos;
            ++bufPos;
            char ch;
            for (; ; ) {
                ch = charAt(++pos);

                if (ch == '`') {
                    bufPos++;
                    ++pos;
                    break;
                } else if (ch == EOI) {
                    throw new ParserException("illegal identifier. " + info());
                }

                bufPos++;
                continue;
            }

            this.ch = charAt(pos);

            stringVal = subString(mark, bufPos);
            token = Token.VARIANT;
        } else {
            for (; ; ) {
                ch = charAt(++pos);

                if (!isIdentifierChar(ch)) {
                    break;
                }

                bufPos++;
                continue;
            }
        }

        this.ch = charAt(pos);

        stringVal = subString(mark, bufPos);
        token = Token.VARIANT;
    }


    public void skipFirstHintsOrMultiCommentAndNextToken() {
        int starIndex = pos + 2;

        for (; ; ) {
            starIndex = text.indexOf('*', starIndex);
            if (starIndex == -1 || starIndex == text.length() - 1) {
                this.token = Token.ERROR;
                return;
            }

            int slashIndex = starIndex + 1;
            if (charAt(slashIndex) == '/') {
                pos = slashIndex + 1;
                ch = text.charAt(pos);
                if (pos < text.length() - 6) {
                    int pos_6 = pos + 6;
                    char c0 = ch;
                    char c1 = text.charAt(pos + 1);
                    char c2 = text.charAt(pos + 2);
                    char c3 = text.charAt(pos + 3);
                    char c4 = text.charAt(pos + 4);
                    char c5 = text.charAt(pos + 5);
                    char c6 = text.charAt(pos_6);
                    if (c0 == 's' && c1 == 'e' && c2 == 'l' && c3 == 'e' && c4 == 'c' && c5 == 't' && c6 == ' ') {
                        this.comments = null;
                        reset(pos_6, ' ', Token.SELECT);
                        return;
                    }

                    if (c0 == 'i' && c1 == 'n' && c2 == 's' && c3 == 'e' && c4 == 'r' && c5 == 't' && c6 == ' ') {
                        this.comments = null;
                        reset(pos_6, ' ', Token.INSERT);
                        return;
                    }

                    if (c0 == 'u' && c1 == 'p' && c2 == 'd' && c3 == 'a' && c4 == 't' && c5 == 'e' && c6 == ' ') {
                        this.comments = null;
                        reset(pos_6, ' ', Token.UPDATE);
                        return;
                    }


                    if (c0 == 'd' && c1 == 'e' && c2 == 'l' && c3 == 'e' && c4 == 't' && c5 == 'e' && c6 == ' ') {
                        this.comments = null;
                        reset(pos_6, ' ', Token.DELETE);
                        return;
                    }

                    if (c0 == 'S' && c1 == 'E' && c2 == 'L' && c3 == 'E' && c4 == 'C' && c5 == 'T' && c6 == ' ') {
                        this.comments = null;
                        reset(pos_6, ' ', Token.SELECT);
                        return;
                    }

                    if (c0 == 'I' && c1 == 'N' && c2 == 'S' && c3 == 'E' && c4 == 'R' && c5 == 'T' && c6 == ' ') {
                        this.comments = null;
                        reset(pos_6, ' ', Token.INSERT);
                        return;
                    }

                    if (c0 == 'U' && c1 == 'P' && c2 == 'D' && c3 == 'A' && c4 == 'T' && c5 == 'E' && c6 == ' ') {
                        this.comments = null;
                        reset(pos_6, ' ', Token.UPDATE);
                        return;
                    }

                    if (c0 == 'D' && c1 == 'E' && c2 == 'L' && c3 == 'E' && c4 == 'T' && c5 == 'E' && c6 == ' ') {
                        this.comments = null;
                        reset(pos_6, ' ', Token.DELETE);
                        return;
                    }

                    nextToken();
                    return;
                } else {
                    nextToken();
                    return;
                }
            }
            starIndex++;
        }
    }


    public void scanComment() {
        Token lastToken = this.token;

        if (ch == '-') {
            char next_2 = charAt(pos + 2);
            if (isDigit(next_2)) {
                scanChar();
                token = Token.SUB;
                return;
            }
        } else if (ch != '/') {
            throw new IllegalStateException();
        }

        mark = pos;
        bufPos = 0;
        scanChar();

        // /*+ */
        if (ch == '*') {
            scanChar();
            bufPos++;

            while (ch == ' ') {
                scanChar();
                bufPos++;
            }

            boolean isHint = false;
            int startHintSp = bufPos + 1;
            if (ch == '!' //
                    || ch == '#' // oceanbase hints
                    ) {
                isHint = true;
                scanChar();
                bufPos++;
            }

            int starIndex = pos;

            for (; ; ) {
                starIndex = text.indexOf('*', starIndex);
                if (starIndex == -1 || starIndex == text.length() - 1) {
                    this.token = Token.ERROR;
                    return;
                }
                if (charAt(starIndex + 1) == '/') {
                    if (isHint) {
                        //stringVal = subString(mark + startHintSp, (bufPos - startHintSp) - 2);
                        stringVal = this.subString(mark + startHintSp, starIndex - startHintSp - mark);
                        token = Token.HINT;
                    } else {
                        if (!optimizedForParameterized) {
                            stringVal = this.subString(mark, starIndex + 2 - mark);
                        }
                        token = Token.MULTI_LINE_COMMENT;
                        commentCount++;
                        if (keepComments) {
                            addComment(stringVal);
                        }
                    }
                    pos = starIndex + 2;
                    ch = charAt(pos);
                    break;
                }
                starIndex++;
            }

            endOfComment = isEOF();

            if (commentHandler != null
                    && commentHandler.handle(lastToken, stringVal)) {
                return;
            }

            return;
        }

        if (ch == '/' || ch == '-') {
            scanChar();
            bufPos++;

            for (; ; ) {
                if (ch == '\r') {
                    if (charAt(pos + 1) == '\n') {
                        bufPos += 2;
                        scanChar();
                        break;
                    }
                    bufPos++;
                    break;
                } else if (ch == EOI) {
                    break;
                }

                if (ch == '\n') {
                    scanChar();
                    bufPos++;
                    break;
                }

                scanChar();
                bufPos++;
            }

            stringVal = subString(mark, bufPos);
            token = Token.LINE_COMMENT;
            commentCount++;
            if (keepComments) {
                addComment(stringVal);
            }

            if (commentHandler != null && commentHandler.handle(lastToken, stringVal)) {
                return;
            }

            endOfComment = isEOF();

            if ((isEOF() || !isSafeComment(stringVal))) {
                throw new NotAllowCommentException();
            }

            return;
        }
    }


    private void scanMultiLineComment() {
        Token lastToken = this.token;

        scanChar();
        scanChar();
        mark = pos;
        bufPos = 0;

        for (; ; ) {
            if (ch == '*' && charAt(pos + 1) == '/') {
                scanChar();
                scanChar();
                break;
            }

            // multiline comment结束符错误
            if (ch == EOI) {
                throw new ParserException("unterminated /* comment. " + info());
            }
            scanChar();
            bufPos++;
        }

        stringVal = subString(mark, bufPos);
        token = Token.MULTI_LINE_COMMENT;
        commentCount++;
        if (keepComments) {
            addComment(stringVal);
        }

        if (commentHandler != null && commentHandler.handle(lastToken, stringVal)) {
            return;
        }

        if (!isSafeComment(stringVal)) {
            throw new NotAllowCommentException();
        }
    }

    private void scanSingleLineComment() {
        Token lastToken = this.token;

        scanChar();
        scanChar();
        mark = pos;
        bufPos = 0;

        for (; ; ) {
            if (ch == '\r') {
                if (charAt(pos + 1) == '\n') {
                    scanChar();
                    break;
                }
                bufPos++;
                break;
            }

            if (ch == '\n') {
                scanChar();
                break;
            }

            // single line comment结束符错误
            if (ch == EOI) {
                throw new ParserException("syntax error at end of input. " + info());
            }

            scanChar();
            bufPos++;
        }

        stringVal = subString(mark, bufPos);
        token = Token.LINE_COMMENT;
        commentCount++;
        if (keepComments) {
            addComment(stringVal);
        }

        if (commentHandler != null && commentHandler.handle(lastToken, stringVal)) {
            return;
        }

        if (!isSafeComment(stringVal)) {
            throw new NotAllowCommentException();
        }
    }

    public void scanIdentifier() {
        hash_lower = 0;
        hash = 0;

        final char first = ch;

        if (ch == 'b'
                && charAt(pos + 1) == '\'') {
            int i = 2;
            int mark = pos + 2;
            for (; ; ++i) {
                char ch = charAt(pos + i);
                if (ch == '0' || ch == '1') {
                    continue;
                } else if (ch == '\'') {
                    bufPos += i;
                    pos += (i + 1);
                    stringVal = subString(mark, i - 2);
                    this.ch = charAt(pos);
                    token = Token.BITS;
                    return;
                } else if (ch == EOI) {
                    throw new ParserException("illegal identifier. " + info());
                } else {
                    break;
                }
            }
        }

        if (ch == '`') {
            mark = pos;
            bufPos = 0;
            char ch;

            int startPos = pos + 1;
            int quoteIndex = text.indexOf('`', startPos);
            if (quoteIndex == -1) {
                throw new ParserException("illegal identifier. " + info());
            }

            hash_lower = FnvHash.BASIC;
            hash = FnvHash.BASIC;

            for (int i = startPos; i < quoteIndex; ++i) {
                ch = text.charAt(i);

                hash_lower ^= ((ch >= 'A' && ch <= 'Z') ? (ch + 32) : ch);
                hash_lower *= FnvHash.PRIME;

                hash ^= ch;
                hash *= FnvHash.PRIME;
            }
            stringVal = SymbolTable.global.addSymbol(text, pos + 1, quoteIndex - pos - 1, hash);
//            stringVal = text.substring(pos, quoteIndex + 1);
            pos = quoteIndex + 1;
            this.ch = charAt(pos);
            token = Token.IDENTIFIER;
        } else {
            final boolean firstFlag = CharTypes.isFirstIdentifierChar(first);
            if (!firstFlag) {
                throw new ParserException("illegal identifier. " + info());
            }

            hash_lower = FnvHash.BASIC;
            ;
            hash = FnvHash.BASIC;
            ;

            hash_lower ^= ((ch >= 'A' && ch <= 'Z') ? (ch + 32) : ch);
            hash_lower *= FnvHash.PRIME;

            hash ^= ch;
            hash *= FnvHash.PRIME;

            mark = pos;
            bufPos = 1;
            char ch = '\0';
            for (; ; ) {
                ch = charAt(++pos);

                if (!isIdentifierChar(ch)) {
                    break;
                }

                bufPos++;

                hash_lower ^= ((ch >= 'A' && ch <= 'Z') ? (ch + 32) : ch);
                hash_lower *= FnvHash.PRIME;

                hash ^= ch;
                hash *= FnvHash.PRIME;

                continue;
            }

            this.ch = charAt(pos);

            if (bufPos == 1) {
                token = Token.IDENTIFIER;
                stringVal = CharTypes.valueOf(first);
                if (stringVal == null) {
                    stringVal = Character.toString(first);
                }
                return;
            }

            Token tok = keywords.getKeyword(hash_lower);
            if (tok != null) {
                token = tok;
                if (token == Token.IDENTIFIER) {
                    stringVal = SymbolTable.global.addSymbol(text, mark, bufPos, hash);
//                    stringVal = text.substring(mark,mark+bufPos);
                } else {
                    stringVal = null;
                }
            } else {
                token = Token.IDENTIFIER;
                stringVal = SymbolTable.global.addSymbol(text, mark, bufPos, hash);
//                stringVal = text.substring(mark,mark+bufPos);
            }

        }
    }


    public void scanNumber() {
        mark = pos;

        if (ch == '0' && charAt(pos + 1) == 'b') {
            int i = 2;
            int mark = pos + 2;
            for (; ; ++i) {
                char ch = charAt(pos + i);
                if (ch == '0' || ch == '1') {
                    continue;
                } else if (ch >= '2' && ch <= '9') {
                    break;
                } else {
                    bufPos += i;
                    pos += i;
                    stringVal = subString(mark, i - 2);
                    this.ch = charAt(pos);
                    token = Token.BITS;
                    return;
                }
            }
        }

        if (ch == '-') {
            bufPos++;
            ch = charAt(++pos);
        }

        for (; ; ) {
            if (ch >= '0' && ch <= '9') {
                bufPos++;
            } else {
                break;
            }
            ch = charAt(++pos);
        }

        boolean isDouble = false;

        if (ch == '.') {
            if (charAt(pos + 1) == '.') {
                token = Token.LITERAL_INT;
                return;
            }
            bufPos++;
            ch = charAt(++pos);
            isDouble = true;

            for (; ; ) {
                if (ch >= '0' && ch <= '9') {
                    bufPos++;
                } else {
                    break;
                }
                ch = charAt(++pos);
            }
        }

        if (ch == 'e' || ch == 'E') {
            bufPos++;
            ch = charAt(++pos);

            if (ch == '+' || ch == '-') {
                bufPos++;
                ch = charAt(++pos);
            }

            for (; ; ) {
                if (ch >= '0' && ch <= '9') {
                    bufPos++;
                } else {
                    break;
                }
                ch = charAt(++pos);
            }

            isDouble = true;
        }

        if (isDouble) {
            token = Token.LITERAL_FLOAT;
        } else {
            if (CharTypes.isFirstIdentifierChar(ch) && !(ch == 'b' && bufPos == 1 && charAt(pos - 1) == '0')) {
                bufPos++;
                for (; ; ) {
                    ch = charAt(++pos);

                    if (!isIdentifierChar(ch)) {
                        break;
                    }

                    bufPos++;
                    continue;
                }

                stringVal = addSymbol();
                token = Token.IDENTIFIER;
            } else {
                token = Token.LITERAL_INT;
            }
        }
    }


    public void scanHexaDecimal() {
        mark = pos;

        if (ch == '-') {
            bufPos++;
            ch = charAt(++pos);
        }

        for (; ; ) {
            if (CharTypes.isHex(ch)) {
                bufPos++;
            } else {
                break;
            }
            ch = charAt(++pos);
        }

        token = Token.LITERAL_HEX;
    }

    public String hexString() {
        return subString(mark, bufPos);
    }

    private final boolean isDigit(char ch) {
        return ch >= '0' && ch <= '9';
    }

    /**
     * Append a character to sbuf.
     */
    private final void putChar(char ch) {
        if (bufPos == buf.length) {
            char[] newsbuf = new char[buf.length * 2];
            System.arraycopy(buf, 0, newsbuf, 0, buf.length);
            buf = newsbuf;
        }
        buf[bufPos++] = ch;
    }

    /**
     * Return the current token's position: a 0-based offset from beginning of the raw input stream (before unicode
     * translation)
     */
    public final int currentPos() {
        return pos;
    }

    /**
     * 返回当前的mark位置。
     */
    public final int currentMark() {
        return mark;
    }

    /**
     * The value of a literal token, recorded as a string. For integers, leading 0x and 'l' suffixes are suppressed.
     */
    public final String stringVal() {
        if (stringVal == null) {
            stringVal = subString(mark, bufPos);
        }
        return stringVal;
    }

    private final void stringVal(StringBuffer out) {
        if (stringVal != null) {
            out.append(stringVal);
            return;
        }

        out.append(text, mark, mark + bufPos);
    }


    public final List<String> readAndResetComments() {
        List<String> comments = this.comments;

        this.comments = null;

        return comments;
    }

    private boolean isOperator(char ch) {
        switch (ch) {
            case '!':
            case '%':
            case '&':
            case '*':
            case '+':
            case '-':
            case '<':
            case '=':
            case '>':
            case '^':
            case '|':
            case '~':
            case ';':
                return true;
            default:
                return false;
        }
    }

    public void reset(int mark, char markChar, Token token) {
        this.pos = mark;
        this.ch = markChar;
        this.token = token;
    }

    public final String paramString() {
        return subString(mark, bufPos);
    }

    public boolean hasComment() {
        return comments != null;
    }

    public int getCommentCount() {
        return commentCount;
    }

    /**
     * 直接跳到结束位置。
     */
    public void skipToEOF() {
        pos = text.length();
        this.token = Token.EOF;
    }

    /**
     * 确认当前token。
     *
     * @param token
     */
    public void check(Token token) {
        if (!(this.token == token)) {
            throw new ParserException("syntax error： expect " + token + ", actual "
                    + token + " " + info());
        }
    }

    /**
     * 跳到最近的Token。
     *
     * @param t
     */
    public void skipTo(Token t) {
        if (token == t) {
            return;
        }
        while (pos < text.length()) {
            nextToken();
            if (token == t) {
                break;
            }
        }
    }

    /**
     * 跳到最近的Token。
     *
     * @param ts
     */
    public void skipTo(Token... ts) {
        for (Token t : ts) {
            if (token == t) {
                return;
            }
        }
        outLoop:
        while (pos < text.length()) {
            nextToken();
            for (Token t : ts) {
                if (token == t) {
                    break outLoop;
                }
            }
        }
    }

    public boolean isEndOfComment() {
        return endOfComment;
    }

    protected boolean isSafeComment(String comment) {
        if (comment == null) {
            return true;
        }
        comment = comment.toLowerCase();
        if (comment.indexOf("select") != -1 //
                || comment.indexOf("delete") != -1 //
                || comment.indexOf("insert") != -1 //
                || comment.indexOf("update") != -1 //
                || comment.indexOf("into") != -1 //
                || comment.indexOf("where") != -1 //
                || comment.indexOf("or") != -1 //
                || comment.indexOf("and") != -1 //
                || comment.indexOf("union") != -1 //
                || comment.indexOf('\'') != -1 //
                || comment.indexOf('=') != -1 //
                || comment.indexOf('>') != -1 //
                || comment.indexOf('<') != -1 //
                || comment.indexOf('&') != -1 //
                || comment.indexOf('|') != -1 //
                || comment.indexOf('^') != -1 //
                ) {
            return false;
        }
        return true;
    }

    protected void addComment(String comment) {
        if (comments == null) {
            comments = new ArrayList<String>(2);
        }
        comments.add(stringVal);
    }

    public String getText() {
        return text;
    }

    public static interface CommentHandler {
        boolean handle(Token lastToken, String comment);
    }

    public static class SavePoint {
        public Token token;
        int bp;
        int sp;
        int np;
        char ch;
        long hash;
        long hash_lower;
        String stringVal;
    }
}
