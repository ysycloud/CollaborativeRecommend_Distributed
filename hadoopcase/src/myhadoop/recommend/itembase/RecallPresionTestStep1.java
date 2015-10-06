package myhadoop.recommend.itembase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import myhadoop.hdfs.HdfsDAO;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * 本类得到两数据集的推荐结果
 */

public class RecallPresionTestStep1 {

	private static int RECOMMENDER_NUM;
	private static int flag;

	public static class Step1_RecallPresionTestMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			String name = split.getPath().getParent().getName();// 判断读的数据集

			if (name.equals("test")) {// 测试集，查全率
				RECOMMENDER_NUM = Recommend.TestRECOMMENDER_NUM;
				flag = 1;
			} else {// 结果集，查准率
				RECOMMENDER_NUM = Recommend.ResultRECOMMENDER_NUM;
				flag = 0;
			}

		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = Recommend.DELIMITER.split(value.toString());

			String uid = tokens[0];
			String itemID = tokens[1];
			String pref = tokens[2];

			Text k = new Text(uid);
			Text v = new Text(itemID + ":" + pref);

			context.write(k, v);

			// 输出：矩阵:uid itemid:pref
		}
	}

	public static class Step1_RecallPresionTestReducer extends
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Map<String, Float> map = new HashMap<String, Float>();
			for (Text line : values) {
				String[] tokens = line.toString().split(":");
				map.put(tokens[0], Float.parseFloat(tokens[1]));
			}
			// 把map集合按升序排序
			List<Entry<String, Float>> enList = new ArrayList<Entry<String, Float>>(
					map.entrySet());
			Collections.sort(enList,
					new Comparator<Map.Entry<String, Float>>() {
						public int compare(Map.Entry<String, Float> o1,
								Map.Entry<String, Float> o2) {
							Float a = o1.getValue() - o2.getValue();
							if (a == 0) {
								return 0;
							} else if (a > 0) {
								return 1;
							} else {
								return -1;
							}
						}
					});

			// 只reduce出指定个最大打分值的key-value。
			int itercount;
			if (enList.size() >= RECOMMENDER_NUM)
				itercount = RECOMMENDER_NUM;
			else
				itercount = enList.size();

			for (int i = 1; i <= itercount; i++) {

				Text k = new Text(key);
				Text v = new Text();
				if(flag==1){  //测试集
					v.set("A," + enList.get(enList.size() - i).getKey()
							+ "," + enList.get(enList.size() - i).getValue());
				}else{	//结果集
					v.set("B," + enList.get(enList.size() - i).getKey()
							+ "," + enList.get(enList.size() - i).getValue());
				}
				
				// 设置key-value为uid A,item,pref
				System.out.println(k.toString()+"	"+v.toString());
				context.write(k, v);
			}
		}
	}


	public static void run1(Map<String, String> path) throws IOException,
			InterruptedException, ClassNotFoundException {
		JobConf conf = Recommend.config();

		String input = path.get("RecallPresionStep1Input1");
		String output = path.get("RecallPresionStep1Output1");

		HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
		hdfs.rmr(output);
		hdfs.rmr(input);
		hdfs.mkdirs(input);
		hdfs.copyFile(path.get("test"), input);

		Job job = new Job(conf);
		job.setJarByClass(RecallPresionTestStep1.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step1_RecallPresionTestMapper.class);
		job.setReducerClass(Step1_RecallPresionTestReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(3);  //这里是在虚拟机下测试，就直接设成节点数

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
	}

	//flag决定用的数据集，flag为1去已看的结果集，flag为2只保留测试中有的数据集
	public static void run2(Map<String, String> path,int which) throws IOException,
			InterruptedException, ClassNotFoundException {
		JobConf conf = Recommend.config();
		String input = null;
		if(which==1)
			input = path.get("RecallPresionStep1Input2_1");
		else
			input = path.get("RecallPresionStep1Input2_2");
		String output = path.get("RecallPresionStep1Output2");

		HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
		hdfs.rmr(output);

		Job job = new Job(conf);
		// job.setJarByClass(RemoveAlreadyRecommend.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step1_RecallPresionTestMapper.class);
		job.setReducerClass(Step1_RecallPresionTestReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
	}
}
