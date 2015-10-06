package myhadoop.recommend.userbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import myhadoop.hdfs.HdfsDAO;
import myhadoop.recommend.itembase.TestMAERecomend.TestMapper;
import myhadoop.recommend.itembase.TestMAERecomend.TestReducer;

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

public class KeepDataInTest {


    public static class KeepDataMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String flag;// A计算结果集 or B测试数据评分集

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

            } else if (flag.equals("test")) {// B源数据评分集uid	itemid	pref

                Text v = new Text("B:" + pref);

                context.write(k, v);
                System.out.println(k.toString() + "  " + v.toString());
            }
        }
        
        //输出：结果矩阵:uid,itemid	A:pref
        	 //测试数据矩阵：uid,itemid	B:pref
    }

    public static class KeepDataReducer extends Reducer<Text, Text, Text, Text> {


		@Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        	
        	Map<String, Float> map = new HashMap<String, Float>();
            
            for (Text line : values) {
            	String[] tokens = line.toString().split(":");
            	map.put(tokens[0], Float.parseFloat(tokens[1]));            	
            }

            if(map.keySet().size() > 1){  //两数据集中都有值，可以保留结果集的数据
        	
            	Text k = new Text(key);
            	Text v = new Text(map.get("A").toString());
            	context.write(k, v);
            	System.out.println(k.toString() + "  " + v.toString());
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = Recommend.config();

        String input1 = path.get("KeepInput1");
        String input2 = path.get("KeepInput2");
        String output = path.get("KeepOutput");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);
        hdfs.rmr(input1);
        hdfs.mkdirs(input1);
        hdfs.copyFile(path.get("test"), input1);
        
        Job job = new Job(conf);
        job.setJarByClass(KeepDataInTest.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(KeepDataMapper.class);
        job.setReducerClass(KeepDataReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        job.setNumReduceTasks(3);  //这里是在虚拟机下测试，就直接设成节点数

        FileInputFormat.setInputPaths(job, new Path(input2), new Path(input1));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

}
