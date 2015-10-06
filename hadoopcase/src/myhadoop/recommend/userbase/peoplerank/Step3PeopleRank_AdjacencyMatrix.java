package myhadoop.recommend.userbase.peoplerank;

import java.io.IOException;
import java.util.Arrays;
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

public class Step3PeopleRank_AdjacencyMatrix {

    private static int nums;// 页面数
    private static float d;// 阻尼系数

    public static class AdjacencyMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            
        	//user1：user2	n
        	System.out.println(values.toString());
            String[] tokens = Step3PeopleRank_PageRankJob.DELIMITER.split(values.toString());
            String[] mykey = tokens[0].split(":");
	        if(Float.parseFloat(tokens[1])>Recommend.RelationTHRESHOLD){ //关系亲密，相互关注
	        	Text k = new Text(mykey[0]);
	            Text v = new Text(mykey[1]);       
	        	context.write(k, v);
	            //user1	user2   表示user1关注user2，当然也会有user2关注user1
			}
        }
    }

    public static class AdjacencyMatrixReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float[] G = new float[nums];// 概率矩阵列
            Arrays.fill(G, (float) (1 - d) / G.length);

            float[] A = new float[nums];// 近邻矩阵列（申请后自动赋0）
            Arrays.fill(A, 0f);
            int sum = 0;// 链出数量
            for (Text val : values) {
                int idx = Integer.parseInt(val.toString());
                A[idx - 1] = 1;  //userid是：1-num，数组下标要-1
                sum++;
            }

            if (sum == 0) {// 分母不能为0
                sum = 1;
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < G.length; i++) { 
                sb.append("," + (float) (G[i] + d * A[i] / sum));//计算概率矩阵的一列
            }

            Text v = new Text(sb.toString().substring(1)); //去除第一个‘，’
            System.out.println(key + ":" + v.toString());
            context.write(key, v);  
            //userid	value为：本user链出的依次1-num这些user的概率矩阵值
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = Step3PeopleRank_PageRankJob.config();

        String input = path.get("Step3Input3");
     //   String input_pr = path.get("input_pr");
        String output = path.get("tmp1");
   //     String page = path.get("page");
    //    String pr = path.get("pr");
        nums = Integer.parseInt(path.get("nums"));// People数
        d = Float.parseFloat(path.get("d"));// 阻尼数

        HdfsDAO hdfs = new HdfsDAO(Step3PeopleRank_PageRankJob.HDFS, conf);
  //      hdfs.rmr(input);
        hdfs.rmr(output);
  //      hdfs.mkdirs(input);
  //      hdfs.mkdirs(input_pr);
  //      hdfs.copyFile(page, input);
  //      hdfs.copyFile(pr, input_pr);

        Job job = new Job(conf);
        job.setJarByClass(Step3PeopleRank_AdjacencyMatrix.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(AdjacencyMatrixMapper.class);
        job.setReducerClass(AdjacencyMatrixReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
