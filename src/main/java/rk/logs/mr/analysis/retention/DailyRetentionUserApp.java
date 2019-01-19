package rk.logs.mr.analysis.retention;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author rk
 * @Date 2018/11/23 20:38
 * @Description: 次日留存率的统计
 *      昨天和前天的数据
 *      在加载原始数据的时候需要加载两天的数据
 *
 **/
public class DailyRetentionUserApp {
    public static void main(String[] args) {
        if (args == null || args.length < 2){
            System.out.println("Parameter Errors! Usage: <inputPath>... <outputPath>");
            System.exit(-1);
        }

        Path outputPath = new Path(args[args.length - 1]);

        try {
            Configuration conf = new Configuration();

//            System.setProperty("HADOOP_USER_NAME","hadoop");
            String jobName = DailyRetentionUserApp.class.getSimpleName();
            Job job = Job.getInstance(conf, jobName);
            job.setJarByClass(DailyRetentionUserApp.class);

//            set input --> mapper
            for(int i = 0; i < args.length-1; i++){
                //添加多个输入路径
                FileInputFormat.addInputPaths(job,args[i]);

            }
            job.setInputFormatClass(TextInputFormat.class);

            job.setMapperClass(DailyRetentionUserApp.DailyRetentionUserMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

//            set output --> reducer
            outputPath.getFileSystem(conf).delete(outputPath,true);
            FileOutputFormat.setOutputPath(job,outputPath);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setReducerClass(DailyRetentionUserReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            job.setNumReduceTasks(1);

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
     *              /standard/data-clean/access/2018/11/22
     *                                          mid                    userid
     * 1000    河北省  邢台区  598d5d7d-e1c4-46a0-92c5-d91366050f12    20202   1       /passpword/getById?id=11      504     /index  2.0.0.3 Firefox 1542880788628
     *  按照 mid+userid为key，时间为value为map的结果输出，到reduce中对应数据
     *
     */
     static class DailyRetentionUserMapper extends Mapper<LongWritable, Text, Text, Text>{
         private String dataDate;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
             FileSplit fs =  (FileSplit) context.getInputSplit();
            String path = fs.getPath().toString();
            int index = path.indexOf("access/");
            dataDate = path.substring(index+7, index+7+10).replaceAll("\\/","-");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String mid = fields[3];
            String userId = fields[4];
            context.write(new Text(mid+"\t"+userId), new Text(dataDate));

        }
    }


    /**
     * <mid+userid, [2018-11-22, 2018-11-23,2018-11-22, 2018-11-23]
     *      如果集合去重之后的size大于1说明，当前用户既在2018-11-22有数据，又在2018-11-23有数据
     *      如果集合去重之后的size小于等于1说明，
     *
     *      通过上述操作，可以计算出在22登陆的用户数，在23登陆的用户数，在22和23号都登陆的用户数
     */
    static class DailyRetentionUserReducer extends Reducer<Text, Text, Text, Text>{

        private Set<String> dateSets;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            dateSets = new HashSet<String>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            dateSets.clear();
            for(Text dateText : values){
                dateSets.add(dateText.toString());
            }
            context.write(key, new Text(dateSets.toString()));

        }
    }






}
