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

public class Step3PeopleRank_SparkPreprocess {

    public static class SparkPreMapper extends Mapper<LongWritable, Text, Text, Text> {

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

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = Step3PeopleRank_PageRankJob.config();

        String input = path.get("Step3SparkInput3");
        String output = path.get("Step3SparkOutput3");

        HdfsDAO hdfs = new HdfsDAO(Step3PeopleRank_PageRankJob.HDFS, conf);
        hdfs.rmr(output);
//        String vdir = "hdfs://192.168.201.11:9000/user/hdfs/recommend/vertices";
//        hdfs.rmr(vdir);
//        hdfs.mkdirs(vdir);
//        hdfs.copyFile(path.get("vertices"), vdir);


        Job job = new Job(conf);
        job.setJarByClass(Step3PeopleRank_SparkPreprocess.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SparkPreMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
