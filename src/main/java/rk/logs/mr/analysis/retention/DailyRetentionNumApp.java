package rk.logs.mr.analysis.retention;

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
 * @Date 2018/11/26 19:52
 * @Description:
 **/
public class DailyRetentionNumApp {
    public static void main(String[] args) {
        if (args == null || args.length < 2){
            System.out.println("Parameter Errors! Usage: <inputPath>  <outputPath>");
            System.exit(-1);
        }

        Path outputPath = new Path(args[args.length - 1]);

        try {
            Configuration conf = new Configuration();

//            System.setProperty("HADOOP_USER_NAME","hadoop");
            String jobName = DailyRetentionNumApp.class.getSimpleName();
            Job job = Job.getInstance(conf, jobName);
            job.setJarByClass(DailyRetentionNumApp.class);

//            set input --> mapper
            for(int i = 0; i < args.length-1; i++){
                //添加多个输入路径
                FileInputFormat.addInputPaths(job,args[i]);
            }
            job.setInputFormatClass(TextInputFormat.class);

            job.setMapperClass(DailyRetentionNumApp.DailyRetentionNumAppMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

//            set output --> reducer
            outputPath.getFileSystem(conf).delete(outputPath,true);
            FileOutputFormat.setOutputPath(job,outputPath);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setReducerClass(DailyRetentionNumApp.DailyRetentionNumAppReducer.class);
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
     * 当前作业的数据源，是DailyRetentionUserApp的输出/output/data-clean/retention/
     *  数据格式：
     *      mid + uid + 用户日志操作日期集合
     *      01572a19-0c5a-4d45-a13f-d0a826b4c7a2	40604	[2018-11-23, 2018-11-22]
     * 留存用户数：
     *  2018-11-22_2018-11-23---->1
     * 昨日用户数：
     *  2018-11-23--->1
     * 前日用户数：
     *  2018-11-22--->1
     */
    static class DailyRetentionNumAppMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            //mid + uid + 用户日志操作日期集合
            String line = value.toString();
            String[] fields = line.split("\t");
            if(fields == null || fields.length < 3){
                return;
            }
            String dataSet = fields[2].replaceAll("\\[|\\]", "");
            String[] splits = dataSet.split(",");
            for(String date : splits){
                context.write(new Text(date.trim()),new IntWritable(1));
            }
            if(splits.length == 2){
                String retention = "";
                if(splits[0].compareTo(splits[1]) < 0){
                    retention = splits[0].trim() + "_" + splits[1].trim();
                }else {
                    retention = splits[1].trim() + "_" + splits[0].trim();
                }
                context.write(new Text(retention),new IntWritable(1));
            }



        }
    }

    static class DailyRetentionNumAppReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
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
