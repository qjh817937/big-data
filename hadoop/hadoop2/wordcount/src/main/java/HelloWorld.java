/**
 * Created with IntelliJ IDEA.
 * User: jianshen
 * Date: 15-3-24
 * Time: 下午4:21
 * To change this template use File | Settings | File Templates.
 */

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class HelloWorld extends Configured implements Tool {

    public static class MapClass extends Mapper<Object, Text, Text, IntWritable>
    {
        public void map(Object key, Text value,Context context)
                throws IOException,InterruptedException {
            String line = value.toString();
            // 没有配置 RecordReader，所以默认采用 line 的实现，
            // key 就是行号，value 就是行内容，
            if (line == null || line.equals(""))
                return;
            String[] words = line.split(" ");
            for (String word: words) {
                context.write( new Text(word), new IntWritable(1));
            }
        }
    }

    public static class ReduceClass extends  Reducer<Text, IntWritable,Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)throws IOException,
                InterruptedException {
            int tmp = 0;
            for (IntWritable val : values) {
                tmp = tmp + val.get();
            }
            context.write(key, new IntWritable(tmp));
        }
    }

    public static void main(String[] args) {
        try {
            int res = ToolRunner.run(new Configuration(),new HelloWorld(), args);
            System.exit(res);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    public int run(String[] args) throws Exception{
        if (args == null || args.length <2) {
            System.out.println("args is: inputpath  outputpath");
            return 1;
        }
        String inputpath = args[0];
        String outputpath = args[1];

        Job job = new Job(new Configuration());
        job.setJarByClass(HelloWorld.class);
        job.setJobName("test");
        job.setOutputKeyClass(Text.class);          // 输出的 key 类型，在 OutputFormat 会检查
        job.setOutputValueClass(IntWritable.class); // 输出的 value 类型，在 OutputFormat 会检查
        job.setJarByClass(HelloWorld.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setNumReduceTasks(2);
        FileInputFormat.setInputPaths(job, new Path(inputpath));  //hdfs 中的输入路径
        FileOutputFormat.setOutputPath(job,new Path(outputpath)); //hdfs 中输出路径

        Date startTime = new Date();
        System.out.println("Job started: " + startTime);
        job.waitForCompletion(true);
        Date end_time = new Date();
        System.out.println("Job ended: " + end_time);
        System.out.println("The job took " +
                (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
        return 0;
    }
}