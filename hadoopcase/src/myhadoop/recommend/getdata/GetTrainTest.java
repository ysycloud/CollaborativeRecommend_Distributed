package myhadoop.recommend.getdata;

import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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


public class GetTrainTest {

	private static int out ;
	
    public static class Step1_ToItemPreMapper extends MapReduceBase implements Mapper<Object, Text, IntWritable, Text> {
        private final static IntWritable k = new IntWritable();
        private final static Text v = new Text();

        @Override
        public void map(Object key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String[] tokens = Recommend.DELIMITER.split(value.toString());
            int userID = Integer.parseInt(tokens[0]);
            String itemID = tokens[1];
            String pref = tokens[2];
            k.set(userID);
            v.set(itemID + ":" + pref);
            output.collect(k, v);
        }
    }

    public static class Step1_ToUserVectorReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
        private final static Text v = new Text();

        @Override
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            StringBuilder sb = new StringBuilder();
            
			Map<String, Float> map = new HashMap<String, Float>();
			while(values.hasNext()) {
				String[] tokens = values.next().toString().split(":");
				map.put(tokens[0], Float.parseFloat(tokens[1]));
			}
			
			List<Entry<String, Float>> enList = new ArrayList<Entry<String, Float>>(map.entrySet());
			
			for(int i = 0;i < enList.size();i++){
				if(out == 1 ){
					if(Float.parseFloat(new Integer((i+1)).toString())/enList.size() < 0.8){ //前80%当训练集
						v.set(enList.get(i).getKey() + "	" + enList.get(i).getValue());
						output.collect(key, v);
						System.out.println(key.toString()+"	"+v.toString());
					}
				}else{
					if(Float.parseFloat(new Integer((i+1)).toString())/enList.size() >= 0.8){ //后20%当测试集
						v.set(enList.get(i).getKey() + "	" + enList.get(i).getValue());
						output.collect(key, v);
						System.out.println((i+1)+"   "+enList.size() +"________"+ key.toString()+"	"+v.toString());
					}
				}
			}			
          //键为用户ID，值为其打过分的所有用户的偏好
            
        }
    }

    public static void run(Map<String, String> path ) throws IOException {
        JobConf conf = Recommend.config();

        
        String input1 = path.get("Input1");
        String input2 = path.get("Input2");
        
        out = Integer.parseInt(path.get("out"));
        String output;
        if(out == 1)
        	output = path.get("Output1");
        else
        	output = path.get("Output2");
        

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);
        hdfs.rmr(input1);
        hdfs.mkdirs(input1);
        hdfs.rmr(input2);
        hdfs.mkdirs(input2);
        hdfs.copyFile(path.get("data"), input1);
        hdfs.copyFile(path.get("test"), input2);

        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Step1_ToItemPreMapper.class);
        conf.setReducerClass(Step1_ToUserVectorReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input1) , new Path(input2));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }

}
