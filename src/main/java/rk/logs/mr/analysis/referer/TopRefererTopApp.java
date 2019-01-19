package rk.logs.mr.analysis.referer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

/**
 * @Author rk
 * @Date 2018/11/27 14:13
 * @Description:
 *      求top10，其实说白了就是求多个文件中最大的10个数
 *
 *
 *     mr中的参数的传递，不能使用全局的静态变量！
 *     不行
 *     mr中传递参数只能通过configuration来进行
 *
 **/
public class TopRefererTopApp {

//    public static int topn;
    public static void main(String[] args) {
        if (args == null || args.length < 2){
            System.out.println("Parameter Errors! Usage: <inputPath>  <outputPath> <topn>");
            System.exit(-1);
        }

        Path outputPath = new Path(args[args.length - 2]);
        int topn = Integer.valueOf(args[args.length-1]);

        try {
            Configuration conf = new Configuration();
            conf.set("topn", topn+"");

//            System.setProperty("HADOOP_USER_NAME","hadoop");
            String jobName = TopRefererTopApp.class.getSimpleName();
            Job job = Job.getInstance(conf, jobName);
            job.setJarByClass(TopRefererTopApp.class);

//            set input --> mapper
            for(int i = 0; i < args.length-2; i++){
                FileInputFormat.addInputPaths(job,args[i]);
            }
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

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }





    }


    static class RefererTopMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

        private TreeSet<String> ts;
        private int topn;
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            //设置一个有序集合，按照字符串中的第二列进行降序排序
            //      /check/init 10795
            ts = new TreeSet<String>(new Comparator<String>() {
                @Override
                public int compare(String x, String y) {
                    String xNumstr = x.split("\t")[1].trim();
                    String yNumstr = y.split("\t")[1].trim();
                    return yNumstr.compareTo(xNumstr);
                }
            });

            topn = Integer.valueOf(context.getConfiguration().get("topn", "10"));

        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            ts.add(line);
            if(ts.size() > topn){
                ts.pollLast();
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (String str : ts){
                context.write(new Text(str), NullWritable.get());
            }

        }
    }

    static class ReferTopReducer extends Reducer<Text, NullWritable, Text, NullWritable>{

        private TreeSet<String> ts;
        private int topn;
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            //设置一个有序集合，按照字符串中的第二列进行降序排序
            //      /check/init 10795
            ts = new TreeSet<String>(new Comparator<String>() {
                @Override
                public int compare(String x, String y) {
                    String xNumstr = x.split("\t")[1].trim();
                    String yNumstr = y.split("\t")[1].trim();
                    return yNumstr.compareTo(xNumstr);
                }
            });

            topn = Integer.valueOf(context.getConfiguration().get("topn", "10"));

        }


        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            String line = key.toString();
            ts.add(line);
            if(ts.size() > topn){
                ts.pollLast();
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (String str : ts){
                context.write(new Text(str), NullWritable.get());
            }
        }
    }

}
