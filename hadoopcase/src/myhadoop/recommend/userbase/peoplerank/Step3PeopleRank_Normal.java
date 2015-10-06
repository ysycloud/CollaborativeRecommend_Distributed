package myhadoop.recommend.userbase.peoplerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

public class Step3PeopleRank_Normal {
	
	//pr值归一化到0-10之间
    public static class NormalMapper extends Mapper<LongWritable, Text, Text, Text> {

        Text k = new Text("1");

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            System.out.println(values.toString());
            context.write(k, values);
        }
    }

    public static class NormalReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> vList = new ArrayList<String>();

            float max = 0f;
            for (Text line : values) {
                vList.add(line.toString());

                String[] vals = Step3PeopleRank_PageRankJob.DELIMITER.split(line.toString());
                float f = Float.parseFloat(vals[1]);
                if(f > max)
                	max = f;
            }

            for (String line : vList) {
                String[] vals = Step3PeopleRank_PageRankJob.DELIMITER.split(line.toString());
                Text k = new Text(vals[0]);
                
                float f = Float.parseFloat(vals[1]);
                Text v = new Text(Step3PeopleRank_PageRankJob.scaleFloat((float) (f / max *10)));
                context.write(k, v);
                
                //输出：userid	pr
                
                System.out.println(k + ":" + v);
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = Step3PeopleRank_PageRankJob.config();

        String input = path.get("input_pr");
        String output = path.get("Step3Output3");

        HdfsDAO hdfs = new HdfsDAO(Step3PeopleRank_PageRankJob.HDFS, conf);
        hdfs.rmr(output);

        Job job = new Job(conf);
        job.setJarByClass(Step3PeopleRank_Normal.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(NormalMapper.class);
        job.setReducerClass(NormalReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);

    }

}
