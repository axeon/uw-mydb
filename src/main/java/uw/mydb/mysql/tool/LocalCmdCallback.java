package uw.mydb.mysql.tool;

/**
 * 在服务器内部直接操作mysql数据库的回调信息。
 *
 * @author axeon
 */
public interface LocalCmdCallback<T> {

    /**
     * 成功时返回结果。
     *
     * @param data
     */
    void onSuccess(T data);

    /**
     * 失败时，返回错误编号和信息。
     *
     * @param errorNo
     * @param message
     */
    void onFail(int errorNo, String message);

}
