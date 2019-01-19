package rk.logs.mr.analysis.retention;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @Author rk
 * @Date 2018/11/26 20:34
 * @Description:
 *      第一步的留存用户
 *          前天目录，昨天目录，统计输出目录
 *       第二步的留存数
 *          第一步的统计输出    最终输出目录
 *
 *      前天目录，昨天目录，统计输出目录    最终输出目录
 *
 **/
public class DailyRetentionApp {
    public static void main(String[] args) {
        if (args == null || args.length < 4){
            System.out.println("Parameter Errors! Usage: <inputPath>... <outputPath>");
            System.exit(-1);
        }

        Path retentionUserOutPath = new Path(args[args.length - 2]);

        try {
            Configuration conf = new Configuration();

//            System.setProperty("HADOOP_USER_NAME","hadoop");
            String jobName = DailyRetentionUserApp.class.getSimpleName();
            Job job = Job.getInstance(conf, jobName);
            job.setJarByClass(DailyRetentionUserApp.class);

//            set input --> mapper
            for(int i = 0; i < args.length-2; i++){
                //添加多个输入路径
                FileInputFormat.addInputPaths(job,args[i]);

            }
            job.setInputFormatClass(TextInputFormat.class);

            job.setMapperClass(DailyRetentionUserApp.DailyRetentionUserMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

//            set output --> reducer
            retentionUserOutPath.getFileSystem(conf).delete(retentionUserOutPath,true);
            FileOutputFormat.setOutputPath(job,retentionUserOutPath);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setReducerClass(DailyRetentionUserApp.DailyRetentionUserReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            job.setNumReduceTasks(1);

//            提交作业
            if(!job.waitForCompletion(true)){
                System.exit(-1);
            }else{
                conf = new Configuration();

//            System.setProperty("HADOOP_USER_NAME","hadoop");
                jobName = DailyRetentionNumApp.class.getSimpleName();
                job = Job.getInstance(conf, jobName);
                job.setJarByClass(DailyRetentionNumApp.class);

//            set input --> mapper
                FileInputFormat.setInputPaths(job,retentionUserOutPath);
                job.setInputFormatClass(TextInputFormat.class);

                job.setMapperClass(DailyRetentionNumApp.DailyRetentionNumAppMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(IntWritable.class);

//            set output --> reducer
                Path outputPath = new Path(args[args.length-1]);
                outputPath.getFileSystem(conf).delete(outputPath,true);
                FileOutputFormat.setOutputPath(job,outputPath);
                job.setOutputFormatClass(TextOutputFormat.class);
                job.setReducerClass(DailyRetentionNumApp.DailyRetentionNumAppReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);

                job.setNumReduceTasks(1);
                //提交作业
                System.exit(job.waitForCompletion(true)? 0: -1);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

}
