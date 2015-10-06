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

public class Step4_Update {
	
	/*
	 * 总之多个key之间必须保证没有相互交叉的计算，mapreduce过程才能无误地进行
	 */

    public static class Step4_PartialMultiplyMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String flag;// A同现矩阵 or B评分矩阵
        
        private final static Text v = new Text();
		private final static Text k = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// 判断读的数据集

            // System.out.println(flag);
        }

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());

            if (flag.equals("step3_2")) {// 同现矩阵user1：user2		n
                String[] v1 = tokens[0].split(":");
                String userID1 = v1[0];
                String userID2 = v1[1];
                String num = tokens[1];

                k.set(userID1);
                v.set("A:" + userID2 + "," + num);
                
                //一共itemnum行
                context.write(k, v);
//                System.out.println(k.toString() + "  " + v.toString());

            } else if (flag.equals("step3_1")) {// 评分矩阵userID		itemID：pref
                String[] v2 = tokens[1].split(":");
                String userID = tokens[0];
                String itemID = v2[0];
                String pref = v2[1];

                k.set(userID);
                v.set("B:" + itemID + "," + pref);

                context.write(k, v);
           //     System.out.println(k.toString() + "  " + v.toString());
            }
        }
        
        //输出：评分矩阵:user	B:itemid,pref
        	 //同现矩阵：user1	A：user2,n
    }

    public static class Step4_AggregateReducer extends Reducer<Text, Text, Text, Text> {
    	
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
   //         System.out.println(key.toString() + ":");

            Map<String, String> mapA = new HashMap<String, String>();
            Map<String, String> mapB = new HashMap<String, String>();

            for (Text line : values) {
                String val = line.toString();
          //      System.out.println(val);

                if (val.startsWith("A:")) { //同现矩阵设置该key（user1）下user2对应的同现次数map
                    String[] kv = Recommend.DELIMITER.split(val.substring(2));
                    mapA.put(kv[0], kv[1]);  //user n

                } else if (val.startsWith("B:")) {	//评分矩阵设置该key（user）下所有itemid对应的评分map
                    String[] kv = Recommend.DELIMITER.split(val.substring(2));
                    mapB.put(kv[0], kv[1]);  //item pref

                }
            }

            //对于某一个key可以计算完全部的mapA和mapB，则不会存在前一种方法step4的不可控制的问题
 //           System.out.println("SUMMA算法最终结果矩阵累加钱的一项矩阵：");            
            float result = 0;
            Iterator<String> iter = mapA.keySet().iterator();
            while (iter.hasNext()) {
                String mapk = iter.next();// userID

                float num = Float.parseFloat(mapA.get(mapk));
                Iterator<String> iterb = mapB.keySet().iterator();
                while (iterb.hasNext()) {
                    String mapkb = iterb.next();// itemID
                    float pref = Float.parseFloat(mapB.get(mapkb));
                    result = num * pref;// 矩阵乘法相乘计算

                    /*
                     * 双重循环完成后输出一个itemnum*usernum的矩阵，完整情况会收itemnum个key，
                     * 就有这么多个这种矩阵，最后的评估结果就是itemnum个这种矩阵的对应位置的累加除以相似度的累加
                     * 
                     * user对item的打分应该是：item同现/相似度矩阵的行乘以user打分矩阵的列再累加/同现矩阵行的累加。
                     * 由矩阵变换和同现矩阵对称性可以分析出这种计算的正确性。
                     * 
                     * 这正是m*k与k*n的矩阵乘法的并行版本，把对k的循环放到外层。
                     * 这就是经典的SUMMA算法，矩阵1的第i列和矩阵2的第i行两两元素相乘，就算出结果矩阵k项累加前的第i项矩阵，
                     * 直到矩阵1的第k列和矩阵2的第k行。本例中应为矩阵1（同现矩阵）是对称的，就都用的两矩阵的第i行双重循环了。
                     */
                    
                    
                    
                    Text k = new Text(mapkb);  
                    Text v = new Text(mapk + "," + result + "," + num);
                    

                    context.write(k, v);
                    System.out.println("该Item："+k.toString() + "本项未累加结果为：" + v.toString());
           
                    
                }
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = Recommend.config();

        String input1 = path.get("Step4Input1");
        String input2 = path.get("Step4Input2");
        String output = path.get("Step4Output");

        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(output);

        Job job = new Job(conf);
        job.setJarByClass(Step4_Update.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4_PartialMultiplyMapper.class);
        job.setReducerClass(Step4_AggregateReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
 //       job.setNumReduceTasks(3);  //这里是在虚拟机下测试，就直接设成节点数

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

}
