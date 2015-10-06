package myhadoop.recommend.userbase.peoplerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import myhadoop.hdfs.HdfsDAO;
import myhadoop.recommend.itembase.RecallPresionTestStep1.Step1_RecallPresionTestMapper;
import myhadoop.recommend.itembase.RecallPresionTestStep1.Step1_RecallPresionTestReducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RecallPresionTestStep2 {
	
	public static class Step2_RecallPresionTestMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = Recommend.DELIMITER.split(value.toString());

			String uid = tokens[0];
			String type = tokens[1];
			String itemID = tokens[2];
			String pref = tokens[3];

			Text k = new Text(uid);
			Text v = new Text(type + "," + itemID + "," + pref);

			context.write(k, v);

			// 输出：矩阵:uid type,itemid,pref
		}
	}

	public static class Step2_RecallPresionTestReducer extends
			Reducer<Text, Text, Text, Text> {

		Float Recallavg = 0f;
		Float Presionavg = 0f;
		int count = 0;
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			Text v = new Text();
			Recallavg /= count;
			Presionavg /= count;
			v.set("recall="+ Recallavg.toString()+";presion="+Presionavg.toString());
			System.out.println("平均计算结果 :"+ "	" + v.toString());					
			context.write(new Text("平均计算结果 :"), v);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Set<String> setA = new HashSet<String>();
			Set<String> setB = new HashSet<String>();
			for (Text line : values) {
				String[] tokens = Recommend.DELIMITER.split(line.toString());
				if(tokens[0].equals("A"))   //测试集
					setA.add(tokens[1]);
				else
					setB.add(tokens[1]);    //结果集
			}
			int sizeA = setA.size();
			int sizeB = setB.size();
			Text v = new Text(); 
			if(sizeA!=0&&sizeB!=0){ //该用户的打分在两个集都有
				boolean ret=setA.retainAll(setB); //从A中只保留B中有的元素
				if(ret){
					System.out.println("same:"+setA.size()+";test:"+sizeA+";result:"+sizeB);
					Float recall = (float)setA.size()/ (float) sizeA;  //查全率
					Recallavg += recall;
					Float presion = (float) setA.size()/ (float) sizeB;  //查准率
					Presionavg += presion;
					count++;
					//v.set("recall="+recall.toString()+";presion="+presion.toString());
					v.set(recall.toString()+"	"+presion.toString());
					System.out.println("该用户"+key.toString()+"的"+v.toString());					
					context.write(key, v);
				}else{ //没有可剔除的，B包含A
					System.out.println("same:"+setA.size()+";test:"+sizeA+";result:"+sizeB);
					count++;
					Recallavg += 1;
					Float presion = (float) setA.size()/ (float) setB.size();  //查准率
					Presionavg += presion;
					//v.set("recall=1;presion="+presion.toString());
					v.set("1	"+presion.toString());
					System.out.println("该用户"+key.toString()+"的"+v.toString());					
					context.write(key, v);
				}
			}
		}
	}

	public static void run(Map<String, String> path) throws IOException,
	InterruptedException, ClassNotFoundException {
		JobConf conf = Recommend.config();

		String input1 = path.get("RecallPresionStep2Input1");
		String input2 = path.get("RecallPresionStep2Input2");
		String output = path.get("RecallPresionStep2Output");

		HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
		hdfs.rmr(output);

		Job job = new Job(conf);
		job.setJarByClass(RecallPresionTestStep2.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step2_RecallPresionTestMapper.class);
		job.setReducerClass(Step2_RecallPresionTestReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(3);  //这里是在虚拟机下测试，就直接设成节点数

		FileInputFormat.setInputPaths(job, new Path(input1),new Path(input2));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
	}

}
