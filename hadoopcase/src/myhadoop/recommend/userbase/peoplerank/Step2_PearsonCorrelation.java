package myhadoop.recommend.userbase.peoplerank;

import java.io.IOException;
import java.util.HashMap;
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

//全准率：0.36862722    误差率：  0.8323325 0,5,5
//      
/*
 * 获取两个项目之间的皮尔逊相似度,相关系数的绝对值越大,相关度越大
 */
public class Step2_PearsonCorrelation {  
	
    public static class Step2_ItemVectorToPearsonMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
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
                    v.set("x:"+x.toString()+",y:"+y.toString()+",x2:"+x2.toString()+",y2:"+y2.toString()+",xy:"+xy.toString());
                    output.collect(k, v);
                }
            }
        }
    }

    public static class Step2_ItemVectorToPearsonReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
   	
    	 private final static Text v = new Text();
    	 
        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            
        	
        	int count = 0;
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
        		count++;
            }
        	
            //pearson相关性公式： p=(Σxy-Σx*Σy/n)/Math.sqrt((Σx2-(Σx)2/n)(Σy2-(Σy)2/n));
            Float sd = map.get("xy") - map.get("x")*map.get("y")/count;  
            Float sm = (float) Math.sqrt((map.get("x2") - Math.pow(map.get("x"), 2)/count)*(map.get("y2") - Math.pow(map.get("y"), 2)/count)); 
            Float result = Math.abs(sm == 0 ? 1 : sd / sm);
        	
            v.set(result.toString());
            
            output.collect(key, v);
            String[] mykey = key.toString().split(":");
            if(!mykey[0].equals(mykey[1])){  //两项目不一致时，再把下半矩阵写出去
            	output.collect(new Text(mykey[1] + ":" + mykey[0]), v);
            }
            //user1：user2	n
  //          if(result>1)
            	System.out.println("两项目："+key.toString()+"的相似度是："+result.toString());
        }
    }

    public static void run(Map<String, String> path) throws IOException {
        JobConf conf = Recommend.config();
        conf.setJarByClass(Step2_PearsonCorrelation.class);

        String input = path.get("Step2Input"); 
        String output = path.get("Step2Output");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);

        conf.setMapperClass(Step2_ItemVectorToPearsonMapper.class);       
        conf.setReducerClass(Step2_ItemVectorToPearsonReducer.class);
     
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
