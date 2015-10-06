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

//全准率：	   误差率：
/*       
* 获取两个项目之间的余弦相似度,余弦值越接近1，就表明夹角越接近0度，也就是两个向量越相似
*/

public class Step2_CosCorrelation {

	 public static class Step2_ItemVectorToCosMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
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
	                    Float x2 = (float) Math.pow( x ,2);
	                    Float y2 = (float) Math.pow( y ,2);
	                    Float xy = x * y ;
	                    v.set("x2:"+x2.toString()+",y2:"+y2.toString()+",xy:"+xy.toString());
	                    output.collect(k, v);
	                }
	            }
	        }
	    }

	    public static class Step2_ItemVectorToCosReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	    	private final static Text v = new Text();
	    	
	        @Override
	        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	            
	            Map<String, Float> map = new HashMap<String, Float>();  //用map记录各项的累加值
	        	while (values.hasNext()) {
	        		String[] tokens = Recommend.DELIMITER.split(values.next().toString());
	        		for(String token:tokens){
	        			String[] entry = token.split(":");
	        		     //  System.out.println(tokens[0] +"		"+ tokens[1]);
	           		 	if (map.containsKey(entry[0])) {
	                        map.put(entry[0], map.get(entry[0]) + Float.parseFloat(entry[1]));//项目出现过，累加
	                    } else {
	                        map.put(entry[0], Float.parseFloat(entry[1]));  //项目还没出现过，创建并标记该初值
	                    }                 
	        		}
	            }
	        	
	            //cos相关性公式： p= Σxy/(Math.sqrt(Σx^2)*Math.sqrt(Σy^2))
	            Float result = (float) (map.get("xy") /( Math.sqrt(map.get("x2")) * Math.sqrt(map.get("y2")) )) ;
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
	        conf.setJarByClass(Step2_CosCorrelation.class);

	        String input = path.get("Step2Input"); 
	        String output = path.get("Step2Output");

	        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
	        hdfs.rmr(output);

	        conf.setMapperClass(Step2_ItemVectorToCosMapper.class);       
	        conf.setReducerClass(Step2_ItemVectorToCosReducer.class);
	     
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	              
	        conf.setInputFormat(TextInputFormat.class);
	        conf.setOutputFormat(TextOutputFormat.class);
	        
	      //  conf.setNumReduceTasks(3);  //这里是在虚拟机下测试，就直接设成节点数

	        FileInputFormat.setInputPaths(conf, new Path(input));
	        FileOutputFormat.setOutputPath(conf, new Path(output));

	        RunningJob job = JobClient.runJob(conf);
	        while (!job.isComplete()) {
	            job.waitForCompletion();
	        }
	    }

}
