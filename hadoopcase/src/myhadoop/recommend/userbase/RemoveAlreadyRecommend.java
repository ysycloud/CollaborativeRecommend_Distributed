package myhadoop.recommend.userbase;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import myhadoop.hdfs.HdfsDAO;

public class RemoveAlreadyRecommend {
	
	/*
	 * 注意：输入路径的目录下必须全是需要处理的源文件，
	 * 如果还有目录的话，mapreduce就会报那个目录名的文件找不到的错误
	 */

    public static class RemoveAlreadyRecommendMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String flag;// A计算结果集 or B源数据评分集

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// 判断读的数据集

            // System.out.println(flag);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(value.toString());
            
            String uid= tokens[0];
            String itemID = tokens[1];
            String pref = tokens[2];

            Text k = new Text(uid+","+itemID);
            
            if (flag.equals("step5")) {// A计算结果集 uid	item，pref

                Text v = new Text("A:" + pref);
                
                context.write(k, v);
                System.out.println(k.toString() + "  " + v.toString());

            } else if (flag.equals("removesource")) {// B源数据评分集uid	itemid	pref

                Text v = new Text("B:" + pref);

                context.write(k, v);
                System.out.println(k.toString() + "  " + v.toString());
            }
        }
        
        //输出：结果矩阵:uid,itemid	A:pref
        	 //源数据矩阵：uid,itemid	B:pref
    }

    public static class RemoveAlreadyRecommendReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
            int count = 0;
            Iterable<Text> tmp = values;
            Text value = new Text();
            for (Text line : tmp) {
            	count++;
            	value = line;
            }

            System.out.println(count);
            if(count == 1){
            	String val = value.toString();
            	System.out.println(val);
            	if(val.startsWith("A:")){
            		//不是两数据源都有，且是计算结果数据，则为未看过的数据,写出
            		Text k = new Text(key);
            		Text v = new Text(val.substring(2));
            		context.write(k, v);
            		System.out.println(k.toString() + "  " + v.toString());
            	}           	
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = Recommend.config();

        String input1 = path.get("RemoveInput1");
        String input2 = path.get("RemoveInput2");
        String output = path.get("RemoveOutput");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);
        hdfs.rmr(input1);
        hdfs.mkdirs(input1);
        hdfs.copyFile(path.get("data"), input1);
        
        Job job = new Job(conf);
        job.setJarByClass(RemoveAlreadyRecommend.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(RemoveAlreadyRecommendMapper.class);
        job.setReducerClass(RemoveAlreadyRecommendReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setNumReduceTasks(3);  //这里是在虚拟机下测试，就直接设成节点数

        FileInputFormat.setInputPaths(job, new Path(input2), new Path(input1));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

}
