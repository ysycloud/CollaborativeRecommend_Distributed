package myhadoop.recommend.itembase;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

//对物品的组合列表进行计数，建立物品的同现矩阵（行列都是项目【用于两项目间相似度】，值为对这两个项目都进行了打分的用户的个数）
//全准率：0.31851825  误差率：0.85244176   0,5,5 ，500
//       0.32000706 0.32200417      0.81512004
public class Step2 {
    public static class Step2_UserVectorToCooccurrenceMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
        private final static Text k = new Text();
        private final static FloatWritable v = new FloatWritable(1);
        
        @Override
        public void map(LongWritable key, Text values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            /********************************
             * TextInputFormat会将每行数据生成一条记录，其中key值为每条记录在分片中的字节偏移量，value则为每行的内容
             * 上次reduce结果写入文件后，默认的形式是一行一条记录，key和value用\t分开
             * 这里用"[\t,]"的正则式进行values分片，\t刚好分割出uid记于tokens[0],而“,”分割出后面的itemid:pref记于tokens[1]之后。
             ******************************/
            //得到某个用户打分过的所有项目和偏好值
            for (int i = 1; i < tokens.length; i++) {
                String itemID = tokens[i].split(":")[0];//得到对应的项目id
                for (int j = i; j < tokens.length; j++) {
                    String itemID2 = tokens[j].split(":")[0];
                    if(Float.parseFloat(tokens[i].split(":")[1])>2.5&&Float.parseFloat(tokens[j].split(":")[1])>2.5){
                    k.set(itemID + ":" + itemID2);
                    output.collect(k, v);	
                    //该用户打过分的任意两个项目为键，值为统计该用户给他们都打过分(>2.5)一下记1，正反都记了一次
                    }
                }
            }
        }
    }

    public static class Step2_UserVectorToConoccurrenceReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();

        @Override
        public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
            float sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            result.set(sum);
            output.collect(key, result);
            String[] mykey = key.toString().split(":");
            if(!mykey[0].equals(mykey[1])){  //两项目不一致时，再把下半矩阵写出去
            	output.collect(new Text(mykey[1] + ":" + mykey[0]), result);
            }
            //item1：item2	n	 
     //       System.out.println(key.toString()+"	"+result.toString());
        }
    }

    public static void run(Map<String, String> path) throws IOException {
        JobConf conf = Recommend.config();
        conf.setJarByClass(Step2.class);

        String input = path.get("Step2Input"); 
        String output = path.get("Step2Output");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);

        conf.setMapperClass(Step2_UserVectorToCooccurrenceMapper.class);   
        //map一部分后就会用它去combiner一下（必须reduce的输出格式允许作为输入再次reduce才行），慎用
        //当然，对于求和这种分配型的计算，用combiner减少map的输出，减少shuffle的网络流量，对效率的提升还是比较客观的
//        conf.setCombinerClass(Step2_UserVectorToConoccurrenceReducer.class);  //居然加了combiner反而变慢了
        conf.setReducerClass(Step2_UserVectorToConoccurrenceReducer.class);
     
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(FloatWritable.class);
              
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        int maxCurrentReduceTasks = conf.getInt("mapred.tasktracker.reduce.tasks.maximum", 1);
        int ReduceTasks = (int) (Recommend.nNode * maxCurrentReduceTasks * 1.75);
        conf.setNumMapTasks( Recommend.nMap );
        conf.setNumReduceTasks( ReduceTasks );  //这里是在虚拟机下测试，就直接设成节点数

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }
}
