package myhadoop.recommend.userbase.peoplerank;

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

public class Step3PeopleRank_PageRank {
    
    private static int nums ;// 页面数
    private static int iternum ;// 当前迭代次数

    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String flag;// tmp1 or result

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// 判断读的数据集
        }

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            System.out.println(iternum + "次:" + values.toString());
            String[] tokens = Step3PeopleRank_PageRankJob.DELIMITER.split(values.toString());

            if (flag.equals("tmp1")) {
                String row = tokens[0];
                for (int i = 1; i < tokens.length; i++) {
                    Text k = new Text(String.valueOf(i));  //行号
                    Text v = new Text(String.valueOf("A:" + (row) + "," + tokens[i]));//列号和邻接值
                    context.write(k, v);
                    //对于邻接矩阵，把同一行map到一个key下
                }

            } else if (flag.equals("pr")) {
                for (int i = 1; i <= nums; i++) {
                    Text k = new Text(String.valueOf(i));
                    Text v = new Text("B:" + tokens[0] + "," + tokens[1]);
                    context.write(k, v); //相当于把pr这个列向量横向拓展，每个key下有一列
                }
            }           
        }

		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			if(iternum==0){ //第一次迭代不会有“pr”目录，自己创pr值
				for (int i = 1; i <= nums; i++) {
					for(int j = 1; j <= nums; j++)
					{
	                    Text k = new Text(String.valueOf(i));
	                    Text v = new Text("B:" + j + ",1" );
	                    context.write(k, v); //相当于把pr这个列向量(每列为1)横向拓展，每个key下有一列
					}
                }
            }
		}
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Integer, Float> mapA = new HashMap<Integer, Float>();
            Map<Integer, Float> mapB = new HashMap<Integer, Float>();
            float pr = 0f;

            for (Text line : values) {
                System.out.println(iternum+"次：	"+key.toString()+"\t"+line);
                String vals = line.toString();

                if (vals.startsWith("A:")) {
                    String[] tokenA = Step3PeopleRank_PageRankJob.DELIMITER.split(vals.substring(2));
                    mapA.put(Integer.parseInt(tokenA[0]), Float.parseFloat(tokenA[1]));
                    //记录邻接矩阵一行的各项
                }

                if (vals.startsWith("B:")) {
                    String[] tokenB = Step3PeopleRank_PageRankJob.DELIMITER.split(vals.substring(2));
                    mapB.put(Integer.parseInt(tokenB[0]), Float.parseFloat(tokenB[1]));
                    //记录整个pr向量
                }
            }

            Iterator<Integer> iterA = mapA.keySet().iterator();
            while (iterA.hasNext()) {
                int idx = iterA.next();
                float A = mapA.get(idx);
                float B = mapB.get(idx);
                pr += A * B;
            }

            context.write(key, new Text(Step3PeopleRank_PageRankJob.scaleFloat(pr)));
            // System.out.println(key + ":" + PageRankJob.scaleFloat(pr));
        }

    }

    public static void run(Map<String, String> path,int iter) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = Step3PeopleRank_PageRankJob.config();

        String input = path.get("tmp1");
        String output = path.get("tmp2");
        String pr = path.get("input_pr");
        nums = Integer.parseInt(path.get("nums"));//页面数
        iternum = iter; //迭代次数
        
        HdfsDAO hdfs = new HdfsDAO(Step3PeopleRank_PageRankJob.HDFS, conf);
        hdfs.rmr(output);

        Job job = new Job(conf);
        job.setJarByClass(Step3PeopleRank_PageRank.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        if(iternum==0)//第一次迭代需自己创pr向量，不接受该输入
        	FileInputFormat.setInputPaths(job, new Path(input));
        else
        	FileInputFormat.setInputPaths(job, new Path(input), new Path(pr));
        
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);

        hdfs.rmr(pr);  //覆盖pr
        hdfs.rename(output, pr);

    }

}
