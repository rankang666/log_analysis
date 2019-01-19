package rk.logs.mr.analysis.referer;

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
 * @Date 2018/11/27 14:56
 * @Description:
 **/
public class TopRefererApp {
    public static void main(String[] args) {
        if (args == null || args.length < 2){
            System.out.println("Parameter Errors! Usage: <inputPath>  <outputPath> <topin> <topn>");
            System.exit(-1);
        }

        Path referSumPath = new Path(args[args.length - 3]);

        try {
            Configuration conf = new Configuration();

//            System.setProperty("HADOOP_USER_NAME","hadoop");
            String jobName = TopRefererSumApp.class.getSimpleName();
            Job job = Job.getInstance(conf, jobName);
            job.setJarByClass(TopRefererSumApp.class);

//            set input --> mapper
            for(int i = 0; i < args.length-3; i++){
                //添加多个输入路径
                FileInputFormat.addInputPaths(job,args[i]);
            }
            job.setInputFormatClass(TextInputFormat.class);

            job.setMapperClass(TopRefererSumApp.TopRefererMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

//            set output --> reducer
            referSumPath.getFileSystem(conf).delete(referSumPath,true);
            FileOutputFormat.setOutputPath(job,referSumPath);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setReducerClass(TopRefererSumApp.TopRefererReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            job.setNumReduceTasks(1);

//            提交作业
            if(!job.waitForCompletion(true)){
                System.exit(-1);
            }else {

                Path outputPath = new Path(args[args.length - 2]);
                int topn = Integer.valueOf(args[args.length-1]);
                conf = new Configuration();
                conf.set("topn", topn+"");

//            System.setProperty("HADOOP_USER_NAME","hadoop");
                jobName = TopRefererTopApp.class.getSimpleName();
                job = Job.getInstance(conf, jobName);
                job.setJarByClass(TopRefererTopApp.class);

//            set input --> mapper
                FileInputFormat.setInputPaths(job,args[args.length-3]);
                job.setInputFormatClass(TextInputFormat.class);

                job.setMapperClass(TopRefererTopApp.RefererTopMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(NullWritable.class);

//            set output --> reducer
                outputPath.getFileSystem(conf).delete(outputPath,true);
                FileOutputFormat.setOutputPath(job,outputPath);
                job.setOutputFormatClass(TextOutputFormat.class);
                job.setReducerClass(TopRefererTopApp.ReferTopReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(NullWritable.class);

                job.setNumReduceTasks(1);

//            提交作业
                System.exit(job.waitForCompletion(true) ? 0 : -1);

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
