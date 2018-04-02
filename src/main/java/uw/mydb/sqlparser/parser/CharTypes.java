package uw.mydb.sqlparser.parser;


import static uw.mydb.sqlparser.parser.LayoutCharacters.EOI;

/**
 * 字符类型，基于druid的修改版本。
 *
 * @author axeon
 */
public class CharTypes {

    private final static boolean[] HEX_FLAGS = new boolean[256];
    private final static boolean[] FIRST_IDENTIFIER_FLAGS = new boolean[256];
    private final static String[] STRING_CACHE = new String[256];
    private final static boolean[] identifierFlags = new boolean[256];
    private final static boolean[] WHITESPACE_FLAGS = new boolean[256];

    static {
        for (char c = 0; c < HEX_FLAGS.length; ++c) {
            if (c >= 'A' && c <= 'F') {
                HEX_FLAGS[c] = true;
            } else if (c >= 'a' && c <= 'f') {
                HEX_FLAGS[c] = true;
            } else if (c >= '0' && c <= '9') {
                HEX_FLAGS[c] = true;
            }
        }
    }

    static {
        for (char c = 0; c < FIRST_IDENTIFIER_FLAGS.length; ++c) {
            if (c >= 'A' && c <= 'Z') {
                FIRST_IDENTIFIER_FLAGS[c] = true;
            } else if (c >= 'a' && c <= 'z') {
                FIRST_IDENTIFIER_FLAGS[c] = true;
            }
        }
        FIRST_IDENTIFIER_FLAGS['`'] = true;
        FIRST_IDENTIFIER_FLAGS['_'] = true;
        FIRST_IDENTIFIER_FLAGS['$'] = true;
    }

    static {
        for (char c = 0; c < identifierFlags.length; ++c) {
            if (c >= 'A' && c <= 'Z') {
                identifierFlags[c] = true;
            } else if (c >= 'a' && c <= 'z') {
                identifierFlags[c] = true;
            } else if (c >= '0' && c <= '9') {
                identifierFlags[c] = true;
            }
        }
        // identifierFlags['`'] = true;
        identifierFlags['_'] = true;
        //identifierFlags['-'] = true; // mysql

        for (int i = 0; i < identifierFlags.length; i++) {
            if (identifierFlags[i]) {
                char ch = (char) i;
                STRING_CACHE[i] = Character.toString(ch);
            }
        }
    }

    static {
        for (int i = 0; i <= 32; ++i) {
            WHITESPACE_FLAGS[i] = true;
        }

        WHITESPACE_FLAGS[EOI] = false;
        for (int i = 0x7F; i <= 0xA0; ++i) {
            WHITESPACE_FLAGS[i] = true;
        }

        WHITESPACE_FLAGS[160] = true; // 特别处理
    }

    public static boolean isHex(char c) {
        return c < 256 && HEX_FLAGS[c];
    }

    public static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    public static boolean isFirstIdentifierChar(char c) {
        if (c <= FIRST_IDENTIFIER_FLAGS.length) {
            return FIRST_IDENTIFIER_FLAGS[c];
        }
        return c != '　' && c != '，';
    }

    public static boolean isIdentifierChar(char c) {
        if (c <= identifierFlags.length) {
            return identifierFlags[c];
        }
        return c != '　' && c != '，';
    }

    public static String valueOf(char ch) {
        if (ch < STRING_CACHE.length) {
            return STRING_CACHE[ch];
        }
        return null;
    }

    /**
     * @return false if {@link LayoutCharacters#EOI}
     */
    public static boolean isWhitespace(char c) {
        return (c <= WHITESPACE_FLAGS.length && WHITESPACE_FLAGS[c]) //
                || c == '　'; // Chinese space
    }

}
