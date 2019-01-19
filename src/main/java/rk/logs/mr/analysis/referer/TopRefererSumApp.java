package rk.logs.mr.analysis.referer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @Author rk
 * @Date 2018/11/27 12:04
 * @Description:
 **/
public class TopRefererSumApp {
    public static void main(String[] args) {
        if (args == null || args.length < 2){
            System.out.println("Parameter Errors! Usage: <inputPath>  <outputPath>");
            System.exit(-1);
        }

        Path outputPath = new Path(args[args.length - 1]);

        try {
            Configuration conf = new Configuration();

//            System.setProperty("HADOOP_USER_NAME","hadoop");
            String jobName = TopRefererSumApp.class.getSimpleName();
            Job job = Job.getInstance(conf, jobName);
            job.setJarByClass(TopRefererSumApp.class);

//            set input --> mapper
            for(int i = 0; i < args.length-1; i++){
                //添加多个输入路径
                FileInputFormat.addInputPaths(job,args[i]);
            }
            job.setInputFormatClass(TextInputFormat.class);

            job.setMapperClass(TopRefererSumApp.TopRefererMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

//            set output --> reducer
            outputPath.getFileSystem(conf).delete(outputPath,true);
            FileOutputFormat.setOutputPath(job,outputPath);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setReducerClass(TopRefererSumApp.TopRefererReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

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
     * 数据源： /standard/data-clean/access/2018/11/23
     *  httpReferer
     *  以外链为key，求wordcount
     */
    static class TopRefererMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if(fields == null || fields.length < 12){
                return;
            }
            String referer = fields[8];
            context.write(new Text(referer),new IntWritable(1));


        }
    }

    static class TopRefererReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable num : values){
                sum += num.get();
            }
            context.write(key,new IntWritable(sum));

        }
    }


}
