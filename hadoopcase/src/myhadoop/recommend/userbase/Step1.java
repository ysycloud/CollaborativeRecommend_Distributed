package myhadoop.recommend.userbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import myhadoop.hdfs.HdfsDAO;


public class Step1 {

    public static class Step1_ToUserPreMapper extends MapReduceBase implements Mapper<Object, Text, IntWritable, Text> {
        private final static IntWritable k = new IntWritable();
        private final static Text v = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String[] tokens = Recommend.DELIMITER.split(value.toString());
            String userID = tokens[0];
            int itemID = Integer.parseInt(tokens[1]);
            String pref = tokens[2];
            k.set(itemID);
            v.set(userID + ":" + pref);
            output.collect(k, v);
        }
    }

    public static class Step1_ToItemVectorReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
        private final static Text v = new Text();

        @Override
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            StringBuilder sb = new StringBuilder();
            while (values.hasNext()) {
                sb.append("," + values.next());
            }
            v.set(sb.toString().replaceFirst(",", "")); //去掉第一个逗号
            output.collect(key, v);
          //键为itemID，值为其打过分的所有用户的偏好
            System.out.println(key.toString()+"	"+v.toString());
        }
    }

    public static void run(Map<String, String> path) throws IOException {
        JobConf conf = Recommend.config();
        conf.setJarByClass(Step1.class);

        String input = "hdfs://192.168.201.11:9000/user/hdfs/recommend/data1"; //提交的处理训练集的目录
        String input1 = path.get("Step1Input");
        String output = path.get("Step1Output");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);
        hdfs.rmr(input1);
        hdfs.mkdirs(input1);
 //       hdfs.copyFile(path.get("data"), input);

        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Step1_ToUserPreMapper.class);
        conf.setCombinerClass(Step1_ToItemVectorReducer.class);
        conf.setReducerClass(Step1_ToItemVectorReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
//        int maxCurrentReduceTasks = conf.getInt("mapred.tasktracker.reduce.tasks.maximum", 1);
//        int ReduceTasks = (int) (2 * maxCurrentReduceTasks * 0.95);
//        conf.setNumReduceTasks(ReduceTasks);
        /*
         * reduce的数目最好等于0.95或1.75乘以工作节点数，再乘以mapred.tasktracker.reduce.tasks.maximum（一般是每个节点的CPU数）
         * 因子为0.95时会让所有的reduce任务立即装载，并当map任务结束时复制它们的输出结果。当因子为1.75时，一些reduce任务会立即
         * 被装载，而其他一些则会等待。更早的节点会较早地完成第一轮reduce 任务并开始第二轮。最慢的节点第二轮不需要处理任何的reduce
         * 任务，这样可以带来更好的负载平衡。
         */
//        conf.setNumReduceTasks(3);  //这里是在虚拟机下测试，就直接设成节点数

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }

}
