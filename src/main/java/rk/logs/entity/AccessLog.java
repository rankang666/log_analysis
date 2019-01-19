package rk.logs.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author rk
 * @Date 2018/11/22 13:09
 * @Description:
 * 解析之后的结果
 *  appid province city mid userid login_type http_request http_referer browser browser_type time
 *
 **/
@Data
//@AllArgsConstructor
//@NoArgsConstructor
public class AccessLog {
    public String appId       ;
    public String province    ;
    public String city        ;
    public String mid         ;
    public String userId      ;
    public int loginType  ;
    public String httpRequest     ;
    public int status      ;
    public String httpReferer;
    public String browserVersion  ;
    public String browserType;
    public long timestamp;

    @Override
    public String toString() {
        return appId + '\t' + province + '\t' + city + '\t' + mid + '\t' + userId + '\t' + loginType+ '\t' + httpRequest + '\t' + status+ '\t' + httpReferer + '\t' + browserVersion + '\t' + browserType + '\t' + timestamp;
    }

}
