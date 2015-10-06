package myhadoop.recommend.itembase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import myhadoop.hdfs.HdfsDAO;

public class Step4_Update2 {

    public static class Step4_RecommendMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        	//输入：uid itemid,result,num
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            Text k = new Text(tokens[0]);
            Text v = new Text(tokens[1]+","+tokens[2]+","+tokens[3]);
            context.write(k, v);
        }
    }

    public static class Step4_RecommendReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
 //       	System.out.println(key.toString() + ":");

        	Map<String, Float> mapNum = new HashMap<String, Float>();  // 相似度累加        	
            Map<String, Float> map = new HashMap<String, Float>();// 结果
            /*
             * 一个key处理完某个user对所有item的打分向量的累加
             */
            
            for (Text line : values) {
 //               System.out.println(line.toString());
                String[] tokens = Recommend.DELIMITER.split(line.toString());
                String itemID = tokens[0];
                Float score = Float.parseFloat(tokens[1]);  //当前项的打分
                Float num = Float.parseFloat(tokens[2]);    //相似/同现值
                //用map结构区分该用户的不同item的打分，不用复杂的if判断了
                 if (map.containsKey(itemID)) {
                     map.put(itemID, map.get(itemID) + score);// item已经出现过，矩阵乘法累加计算
                     mapNum.put(itemID, mapNum.get(itemID) + num);
                 } else {
                     map.put(itemID, score);  //item还没出现过，创建该item并标记该初值
                     mapNum.put(itemID, num);
                 }
            }
                        
            Iterator<String> iter = map.keySet().iterator();
            Iterator<String> iterNum = mapNum.keySet().iterator();
            while (iter.hasNext()) {
                String itemID = iter.next();
                iterNum.next();
                float score = map.get(itemID);
                float num = mapNum.get(itemID);
                Text v = new Text();
                if(num==0){  //当该user的其他项目与本项目的相似度都为0时，则中立地打一个中间分2.5
                	v.set(itemID + "," + new Float(2.5f).toString());                                     
                }else{
                	v.set(itemID + "," + score/num);                                     
                }
                System.out.println(key.toString()+"	"+v.toString()); 
                context.write(key, v);
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = Recommend.config();

        String input = path.get("Step5Input");
        String output = path.get("Step5Output");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);

        int maxCurrentReduceTasks = conf.getInt("mapred.tasktracker.reduce.tasks.maximum", 1);
        int ReduceTasks = (int) (Recommend.nNode * maxCurrentReduceTasks * 1.75);
        conf.setNumMapTasks( Recommend.nMap );
        conf.setNumReduceTasks( ReduceTasks );  //这里是在虚拟机下测试，就直接设成节点数
        
        Job job = new Job(conf);
        job.setJarByClass(Step4_Update2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4_RecommendMapper.class);
        job.setReducerClass(Step4_RecommendReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
      //  job.setNumReduceTasks(3);  //这里是在虚拟机下测试，就直接设成节点数

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

}
