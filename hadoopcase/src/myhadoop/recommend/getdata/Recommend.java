package myhadoop.recommend.getdata;


/*
 * 求相似度矩阵能够想到的朴素做法：
 * 用一个全局的Map（数据结构）存每个item和和对应的userVector
 * 在Map的cleanup函数中两次迭代上面的Map再进行两两之间的相似度
 * （太耗内存还完全没有体现出MapReduce框架的优势）
 */

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;
import myhadoop.hdfs.HdfsDAO;

public class Recommend {

    public static final String HDFS = "hdfs://192.168.201.11:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    
    public static void main(String[] args) throws Exception {
        Map<String, String> path = new HashMap<String, String>();
        path.put("data", "logfile/u1.base");
        path.put("test", "logfile/u1.test");

        path.put("Input1", HDFS + "/user/hdfs/recommend/sourcedata/train");
        path.put("Input2", HDFS + "/user/hdfs/recommend/sourcedata/test");
        path.put("Output1", HDFS + "/user/hdfs/recommend/sourcedata" + "/trainresult");
        path.put("Output2", HDFS + "/user/hdfs/recommend/sourcedata" + "/testresult");
        
        path.put("out", "1");    //写训练集
 //       path.put("out", "2");     //写测试集
        GetTrainTest.run(path);            
        System.exit(0);
    }

    public static JobConf config() {
        JobConf conf = new JobConf(Recommend.class);
        conf.setJobName("Recommand");
        conf.addResource("resources/hadoop/core-site.xml");
        conf.addResource("resources/hadoop/hdfs-site.xml");
 //       conf.addResource("resources/hadoop/mapred-site.xml");
  //      conf.set("mapred.job.tracker", "hdfs://192.168.201.11:9001");
  //      conf.setJarByClass(Recommend.class);
   //     conf.set("io.sort.mb", "1024");
        return conf;
    }

}
