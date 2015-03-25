package secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Date;

public class SecondarySortMain extends Configured implements Tool {

    public static class MapClass extends Mapper<Object, Text, StringPair, Text>
    {
        public void map(Object key, Text value,Context context)
                throws IOException,InterruptedException {
            // 没有配置 RecordReader，所以默认采用 line 的实现，
            // key 就是行号，value 就是行内容，
            String line = value.toString();
            if (line == null || line.equals(""))
                return;
            String id = "";
            Long timestamp = 0L;
            String[] kvs = line.split(",");
            for (String kvString: kvs) {
                String[] kv = kvString.split("=");
                if(kv[0].equals("id")) {
                    id = kv[1];
                }
                else if(kv[0].equals("timestamp")) {
                    timestamp = Long.parseLong(kv[1]);
                }
            }
            if (id.equals("") || timestamp.equals("")) {
                System.out.println("id/timestamp is empty, line=[" + line + "]");
                return ;
            }
            System.out.println("id=" + id + ",timestamp=" + timestamp);

            StringPair sp = new StringPair(id, timestamp);
            context.write(sp, new Text(line));
        }
    }

    public static class ReduceClass extends Reducer<StringPair, Text,Text, Text>
    {
        public void reduce(StringPair key, Iterable<Text> values,
                           Context context)throws IOException,
                InterruptedException {
            String tmp = "";
            for (Text val : values) {
                tmp = tmp + "," + val.toString();
            }
            context.write(new Text(key.getFirst()), new Text(tmp));
        }
    }

    public static void main(String[] args) {
        try {
            int res = ToolRunner.run(new Configuration(), new SecondarySortMain(), args);
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
        job.setJarByClass(SecondarySortMain.class);
        job.setJobName("test");

        job.setMapOutputKeyClass(StringPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);          // 输出的 key 类型，在 OutputFormat 会检查
        job.setOutputValueClass(Text.class);       // 输出的 value 类型，在 OutputFormat 会检查
        job.setJarByClass(SecondarySortMain.class);

        job.setMapperClass(MapClass.class);
        job.setPartitionerClass(DataPartitioner.class);
        job.setGroupingComparatorClass(DataGroupComparator.class);
        job.setReducerClass(ReduceClass.class);

        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, new Path(inputpath));  //hdfs 中的输入路径
        FileOutputFormat.setOutputPath(job, new Path(outputpath)); //hdfs 中输出路径

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
