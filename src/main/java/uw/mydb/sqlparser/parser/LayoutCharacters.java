package uw.mydb.sqlparser.parser;

/**
 * 基于druid的修改。
 *
 * @author axeon
 */
public interface LayoutCharacters {

    /**
     * Tabulator column increment.
     */
    final static int TAB_INC = 8;

    /**
     * Tabulator character.
     */
    final static byte TAB = 0x8;

    /**
     * Line feed character.
     */
    final static byte LF = 0xA;

    /**
     * Form feed character.
     */
    final static byte FF = 0xC;

    /**
     * Carriage return character.
     */
    final static byte CR = 0xD;

    /**
     * End of input character. Used as a sentinel to denote the character one beyond the last defined character in a
     * source file.
     */
    final static byte EOI = 0x1A;
}
