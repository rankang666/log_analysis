package rk.logs.writable;

/**
 * @Author rk
 * @Date 2018/11/22 12:58
 * @Description:
 *  appid ip mid userid login_type request status http_referer user_agent time
 **/
public class AccessLogWritable {
//    设置的来源   web:1000,android:1001,ios:1002,ipad:1003
    public static final int INDEX_APPID              = 0;
//    ip
    public static final int INDEX_IP                 = 1;
//    标识id
    public static final int INDEX_MID                = 2;
//    用户id
    public static final int INDEX_USERID             = 3;
//    是否登录  1登录   0未登录
    public static final int INDEX_LOGIN_TYPE         = 4;
    public static final int INDEX_REQUEST            = 5;
    public static final int INDEX_STATUS             = 6;
    public static final int INDEX_HTTP_REFERER       = 7;
    public static final int INDEX_USER_AGENT         = 8;
    public static final int INDEX_TIME               = 9;
}
