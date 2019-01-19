package rk.logs.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import redis.clients.jedis.Jedis;
import rk.logs.constants.Constants;
import rk.logs.entity.AccessLog;
import rk.logs.utils.IP;
import rk.logs.utils.UserAgent;
import rk.logs.utils.UserAgentUtil;
import rk.logs.writable.AccessLogWritable;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @Author rk
 * @Date 2018/11/22 10:58
 * @Description:    清洗：http中的access日志信息
 *  原始日志
 *  appid ip mid userid login_type request status http_referer user_agent time
 *  清洗之后的标准
 *  appid province city mid userid login_type http_request http_referer browser browser_type time
 *
 *  没有聚合，就不需要reducer
 *
 *  /input/data-clean/access/2018/11/21 /standard/data-clean/access/2018/11/21
 **/
public class HttpAccessLogCleanApp {
    public static void main(String[] args) {
        if (args == null || args.length < 2){
            System.out.println("Parameter Errors! Usage: <inputPath> <outputPath>");
            System.exit(-1);
        }
        String inputPath = args[0];
        Path outputPath = new Path(args[1]);

        try {
            Configuration conf = new Configuration();

//            System.setProperty("HADOOP_USER_NAME","hadoop");
            String jobName = HttpAccessLogCleanApp.class.getSimpleName();
            Job job = Job.getInstance(conf, jobName);
            job.setJarByClass(HttpAccessLogCleanApp.class);

//            set input --> mapper
            FileInputFormat.setInputPaths(job,inputPath);
            job.setInputFormatClass(TextInputFormat.class);

            job.setMapperClass(AccessLogCleanMapper.class);
//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(NullWritable.class);

//            set output --> reducer
            outputPath.getFileSystem(conf).delete(outputPath,true);
            FileOutputFormat.setOutputPath(job,outputPath);
            job.setOutputFormatClass(TextOutputFormat.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            job.setNumReduceTasks(0);

//            提交作业
            System.exit(job.waitForCompletion(true) ? 0 : -1);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


    }

    /**
     * mr的设计步骤：
     *  step 1 就是确定mapper的类型参数
     *  step 2 覆盖map方法
     *
     *  说明：理论上ip归属地的查询需要使用的在线查询或者本地ip数据的检索，但是效率或者网络的问题，
     *   一般不建议使用，都是相关的信息做成映射表存储在redis中。
     */
    static class AccessLogCleanMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
//        appid ip mid userid login_type request status http_referer user_agent time

        Jedis jedis = null;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
//            IP.load("data/17monipdb.dat");
            jedis = new Jedis("hadoop02",6379);

        }

        private AccessLog aLog = null;
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if(fields == null || fields.length < 10){
               return;
            }

            aLog = new AccessLog();
            String appId = fields[AccessLogWritable.INDEX_APPID];
            aLog.setAppId(appId);
//            ip需要转化
            String ip = fields[AccessLogWritable.INDEX_IP];
//            IP.find(ip);
//            String province = IpUtil.getProvince(ip);
//            发起一次http的请求：{"code":0,"data":{"ip":"210.75.225.254","country":"中国","area":"","region":"北京","city":"北京","county":"XX","isp":"科技网","country_id":"CN","area_id":"","region_id":"110000","city_id":"110100","county_id":"xx","isp_id":"1000114"}}
            String pc = jedis.hget(Constants.IP_REDIS_KEY, ip);
            if(pc != null){
                aLog.setProvince(pc.split("\\|")[0]);
                aLog.setCity(pc.split("\\|")[1]);
            }
            
            String mid = fields[AccessLogWritable.INDEX_MID];
            aLog.setMid(mid);
            String userId = fields[AccessLogWritable.INDEX_USERID];
            aLog.setUserId(userId);
            try{
                String login = fields[AccessLogWritable.INDEX_LOGIN_TYPE];
                aLog.setLoginType(Integer.valueOf(login));
            } catch (NumberFormatException e) {
                aLog.setLoginType(0);
            }
//            request需要转化   GET /top HTTP/1.1
            String request = fields[AccessLogWritable.INDEX_REQUEST];
            String[] splits = request.split(" ");
            if(splits != null && splits.length == 3){
                aLog.setHttpRequest(splits[1]);
            }else {
                aLog.setHttpRequest(" ");
            }

            try{
                String status = fields[AccessLogWritable.INDEX_STATUS];
                aLog.setStatus(Integer.valueOf(status));
            } catch (NumberFormatException e) {
                aLog.setStatus(-1);
            }
            String referer = fields[AccessLogWritable.INDEX_HTTP_REFERER];
            aLog.setHttpReferer(referer);
//            ua需要进行转化
            String agent = fields[AccessLogWritable.INDEX_USER_AGENT];
            UserAgent ua = UserAgentUtil.getUserAgent(agent);
            if(ua != null){
                aLog.setBrowserVersion(ua.getBrowserVersion());
                aLog.setBrowserType(ua.getBrowserType());
            }

            try{
                String time = fields[AccessLogWritable.INDEX_TIME];
                aLog.setTimestamp(Long.valueOf(time));
            } catch (NumberFormatException e) {
                aLog.setTimestamp(-1);
            }
            context.write(new Text(aLog.toString()), NullWritable.get());
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            jedis.close();
        }
    }






}
