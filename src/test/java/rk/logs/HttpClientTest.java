package rk.logs;


import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * @Author rk
 * @Date 2018/11/22 14:56
 * @Description:
 **/
public class HttpClientTest {
    public static void main(String[] args) throws IOException {
        HttpClient client = HttpClients.createDefault();
//        http://ip.taobao.com/service/getIpInfo.php?ip=210.75.225.254
        HttpGet request = new HttpGet("http://ip.taobao.com/service/getIpInfo.php?ip=222.90.232.130");
        HttpResponse response = client.execute(request);
        System.out.println(response.getStatusLine());
        System.out.println(EntityUtils.toString(response.getEntity()));

    }
}
