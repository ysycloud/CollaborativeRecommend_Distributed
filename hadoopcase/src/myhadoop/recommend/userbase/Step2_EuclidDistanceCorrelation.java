package myhadoop.recommend.userbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import myhadoop.hdfs.HdfsDAO;

import org.apache.hadoop.fs.Path;
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

//全准率：   误差率：  0,5,5
/*        
* 获取两个项目之间的欧几里得距离,距离越小越好.
*/

public class Step2_EuclidDistanceCorrelation {

	 public static class Step2_ItemVectorToEuclidMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	        private final static Text k = new Text();
	        private final static Text v = new Text();

	        @Override
	        public void map(LongWritable key, Text values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	            String[] tokens = Recommend.DELIMITER.split(values.toString());
	            System.out.println(values.toString());
	            for (int i = 1; i < tokens.length; i++) {
	                String userID = tokens[i].split(":")[0];
	                String sx = tokens[i].split(":")[1];
	                Float x = Float.parseFloat(sx);
	                for (int j = i; j < tokens.length; j++) {  //只map右上的矩阵，减小计算量
	                    String userID2 = tokens[j].split(":")[0];
	                    String sy = tokens[j].split(":")[1];               
	                    Float y = Float.parseFloat(sy);
	                    k.set(userID + ":" + userID2);
	                    Float xy = (float) Math.pow(x - y, 2); 
	                    v.set(xy.toString());
	                    output.collect(k, v);
	                }
	            }
	        }
	    }

	    public static class Step2_ItemVectorToEuclidReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	    	
	    	 private final static Text v = new Text();
	    	
	        @Override
	        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	            
	            float sum = 0f;  
	        	while (values.hasNext()) {
	        		sum += Float.parseFloat(values.next().toString());
	        		
	            }
	        	
	            //欧几里得距离公式： p= Math.sqrt(Σ(x-y)^2) 本例显然>1	 (因为任意两项相减不是0就是大于1)           
	            Float result = (float) Math.sqrt(sum);
	        	if(result==0){  //相关性与距离相反
	        		result = 1f;
	        	}else{
	        		result = 1/result;
	        	}
	        	
	            v.set(result.toString());	            
	            output.collect(key, v);
	            String[] mykey = key.toString().split(":");
	            if(!mykey[0].equals(mykey[1])){  //两user不一致时，再把下半矩阵写出去
	            	output.collect(new Text(mykey[1] + ":" + mykey[0]), v);
	            }
	            //user1：user2	n
	            if(result>1)
	            	System.out.println(key.toString()+"	"+result.toString());
	        }
	    }

	    public static void run(Map<String, String> path) throws IOException {
	        JobConf conf = Recommend.config();
	        conf.setJarByClass(Step2_EuclidDistanceCorrelation.class);

	        String input = path.get("Step2Input"); 
	        String output = path.get("Step2Output");

	        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
	        hdfs.rmr(output);

	        conf.setMapperClass(Step2_ItemVectorToEuclidMapper.class);       
	        conf.setReducerClass(Step2_ItemVectorToEuclidReducer.class);
	     
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	              
	        conf.setInputFormat(TextInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);
	        
	   //     conf.setNumReduceTasks(3);  //这里是在虚拟机下测试，就直接设成节点数

	        FileInputFormat.setInputPaths(conf, new Path(input));
	        FileOutputFormat.setOutputPath(conf, new Path(output));

	        RunningJob job = JobClient.runJob(conf);
	        while (!job.isComplete()) {
	            job.waitForCompletion();
	        }
	    }

}
