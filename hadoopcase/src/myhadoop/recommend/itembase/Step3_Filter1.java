package myhadoop.recommend.itembase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import myhadoop.hdfs.HdfsDAO;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Step3_Filter1 {

	   public static class Filter1Mapper extends Mapper<LongWritable, Text, Text, Text> {

		private final static Text k = new Text();
		private final static Text v = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//item1：item2	n	
			String[] tokens = Recommend.DELIMITER.split(value.toString());
	        String[] mykey = tokens[0].split(":");
	        k.set(mykey[0]);
	        if(Float.parseFloat(tokens[1])>Recommend.THRESHOLD){ //局部 过滤
	        	v.set(mykey[1]+","+tokens[1]);        
	        	context.write(k, v);
	            //item1	item2，n	
			}
		}
	}

	    public static class Filter1FindKReducer extends Reducer<Text, Text, Text, FloatWritable> {

			private final static Text k = new Text();
			private final static FloatWritable v = new FloatWritable();
			
			@Override
			protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				Map<String, Float> map = new HashMap<String, Float>();
				for(Text line:values) {
					String[] tokens = Recommend.DELIMITER.split(line.toString());
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

				// 只reduce出k个近邻数目的key-value。
				int itercount;
				if (enList.size() >= Recommend.KNEIGHBOUR)
					itercount = Recommend.KNEIGHBOUR;
				else
					itercount = enList.size();

				int count = 0;
				for (int i = 1; i <= itercount; i++) {

					k.set(key + ":" + enList.get(enList.size() - i).getKey());
					v.set(enList.get(enList.size() - i).getValue());
					// 设置key-value为item1：item2 n
					context.write(k, v);
					System.out.println(++count+"	" + k.toString() + "-->" + v.toString());
				}
				// 输出item1：item2 n
			}
	    }

	    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
	        JobConf conf = Recommend.config();

	        String input = path.get("Step3Input2");
	        String output = path.get("Step3Output2");

	        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
	        hdfs.rmr(output);
	        
	        int maxCurrentReduceTasks = conf.getInt("mapred.tasktracker.reduce.tasks.maximum", 1);
	        int ReduceTasks = (int) (Recommend.nNode * maxCurrentReduceTasks * 1.75);
	        conf.setNumMapTasks( Recommend.nMap );
	        conf.setNumReduceTasks( ReduceTasks );  //这里是在虚拟机下测试，就直接设成节点数
	        
	        Job job = new Job(conf);
	        job.setJarByClass(Step3_Filter1.class);

	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(FloatWritable.class);
	        

	        job.setMapperClass(Filter1Mapper.class);
	        job.setReducerClass(Filter1FindKReducer.class);

	        job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);        

	        FileInputFormat.setInputPaths(job, new Path(input));
	        FileOutputFormat.setOutputPath(job, new Path(output));

	        job.waitForCompletion(true);
	    }

	}
