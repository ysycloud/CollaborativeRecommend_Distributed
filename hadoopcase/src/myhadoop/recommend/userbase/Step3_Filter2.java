package myhadoop.recommend.userbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import myhadoop.hdfs.HdfsDAO;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Step3_Filter2 {

	   public static class Filter2Mapper extends Mapper<LongWritable, Text, NewK2, Text> {
		   	
		private final static Map<String, List<Correlation>> CorrelationMatrix = new HashMap<String, List<Correlation>>();
		private final static NewK2 k = new NewK2();
		private final static Text v = new Text();
		    
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			float sum;
			int count;

			for(String user:CorrelationMatrix.keySet()){
				sum = 0f;
				count = 0;
				for (Correlation co : CorrelationMatrix.get(user)) {
					sum += co.getNum();
					count++; 
				}
				float sim = sum/count;  //计算本key的项目与其他项目的平均相似度
				System.out.println(user + "与其他项目的平均相似度是：" + sim);
				if(sim >= Recommend.THRESHOLD){
					for (Correlation co : CorrelationMatrix.get(user)) {
						k.setUser(Integer.parseInt(user));
						k.setSim(sim);
			            v.set(co.getUserID()+","+co.getNum());
			            context.write(k, v);
			            //输出：user1:sim user2，n
			             System.out.println(k.toString()+"	"+v.toString());
			        }
				}
			}			
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//输入：user1：user2	n
			//过滤平均相似度，传入Map中只完成存储，close中确保所有数据存完了才计算过滤
	        String[] tokens = Recommend.DELIMITER.split(value.toString());
	        String[] mykey = tokens[0].split(":");
		    List<Correlation> list = null;
            if (!CorrelationMatrix.containsKey(mykey[0])) {
                list = new ArrayList<Correlation>();
            } else {
                list = CorrelationMatrix.get(mykey[0]);
            }
            list.add(new Correlation(mykey[1], Float.parseFloat(tokens[1])));
            CorrelationMatrix.put(mykey[0], list);
		}	     
	 }

	    public static class Filter2FindKReducer extends Reducer<NewK2, Text, Text, FloatWritable> {

	    	private int count = 0;
	    	
			@Override
			protected void reduce(NewK2 key, Iterable<Text> values, Context context) 
					throws IOException, InterruptedException {
				if (count++ < Recommend.KNEIGHBOUR) { // 收到的key已经按sim的降序排列
				//	System.out.println(count + "	key=" + key.toString());
					int user1 = key.getUser();
					for(Text line:values) {
						String[] tokens = Recommend.DELIMITER.split(line.toString());
						Text k = new Text(user1 + ":" + tokens[0]);
						FloatWritable v = new FloatWritable(
								Float.parseFloat(tokens[1]));
						context.write(k, v);
						System.out.println(count+"	"+k.toString() + "-->" + v.toString());
						// 输出user1：user2 n
					}
				}
				
			}
	    	
	    }

	    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
	        JobConf conf = Recommend.config();

	        String input = path.get("Step3Input2");
	        String output = path.get("Step3Output2");

	        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
	        hdfs.rmr(output);
	        
	        Job job = new Job(conf);
	        job.setJarByClass(Step3_Filter2.class);

	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);

	        job.setMapperClass(Filter2Mapper.class);
	        job.setReducerClass(Filter2FindKReducer.class);
	        
	        job.setMapOutputKeyClass(NewK2.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(FloatWritable.class);
	        
	        
//	        job.setNumReduceTasks(3);  //这里是在虚拟机下测试，就直接设成节点数

	        FileInputFormat.setInputPaths(job, new Path(input));
	        FileOutputFormat.setOutputPath(job, new Path(output));

	        job.waitForCompletion(true);
	    }

	    /*
	     * 定义新的k2类型，这样在map到reduce的过程会自动按k2的降序排序 而且这种要比较的k2最好里面都是数字的成员，不要有Strinf这种的。
	     */
	    static class NewK2 implements WritableComparable<NewK2> {

	    	Integer user;
	    	Float sim;

	    	public String toString() {
	    		return user.toString() + ":" + sim.toString();
	    	}

	    	public Integer getUser() {
	    		return user;
	    	}

	    	public void setUser(Integer user) {
	    		this.user = user;
	    	}

	    	public Float getSim() {
	    		return sim;
	    	}

	    	public void setSim(Float sim) {
	    		this.sim = sim;
	    	}

	    	public NewK2() {
	    	}

	    	public NewK2(Integer first, Float second) {
	    		this.user = first;
	    		this.sim = second;
	    	}

	    	@Override
	    	public void readFields(DataInput in) throws IOException {
	    		this.user = in.readInt();
	    		this.sim = in.readFloat();
	    	}

	    	@Override
	    	public void write(DataOutput out) throws IOException {
	    		out.writeInt(user);
	    		out.writeFloat(sim);
	    	}

	    	/**
	    	 * 当k2进行排序时，会调用该方法. 按sim的降序排
	    	 */
	    	@Override
	    	public int compareTo(NewK2 o) {
	    		final Float minus = this.sim - o.sim;
	    		if (minus != 0) {
	    			if (minus > 0) {
	    				return -1;
	    			} else {
	    				return 1;
	    			}
	    		}
	    		return 0;
	    	}

	    	@Override
	    	public int hashCode() {
	    		return this.user.hashCode() + this.sim.hashCode();
	    	}

	    	@Override
	    	public boolean equals(Object obj) {
	    		if (!(obj instanceof NewK2)) {
	    			return false;
	    		}
	    		NewK2 oK2 = (NewK2) obj;
	    		return (this.user == oK2.user) && (this.sim == oK2.sim);
	    	}
	    }

}

class Correlation {
    private String userID;
    private float num;

    public Correlation(String userID, float num) {
        super();
        this.userID = userID;
        this.num = num;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public float getNum() {
        return num;
    }

    public void setNum(float num) {
        this.num = num;
    }

}
