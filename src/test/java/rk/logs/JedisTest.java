package rk.logs;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author rk
 * @Date 2018/11/22 16:01
 * @Description:
 **/
public class JedisTest {

    private Jedis jedis;
    @Before
    public void setUp(){
        jedis = new Jedis("hadoop",6379);

    }
    @Test
    public void writeIpInfo2Redis() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("data/ip.data"));
        String line = null;
        Map<String, String> ipMap = new HashMap<>();
        while((line = br.readLine()) != null){
            String[] fields = line.split("\t");
            if(fields == null || fields.length != 3){
                continue;
            }
            String ip = fields[0];
            String province = fields[1];
            String city = fields[2];
            ipMap.put(ip,province+"|"+city);
        }

        jedis.hmset("ip_map", ipMap);
        br.close();
    }

    @After
    public void cleanUp(){
        jedis.close();
    }

}
