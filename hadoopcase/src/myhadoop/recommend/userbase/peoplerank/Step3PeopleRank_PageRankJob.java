package myhadoop.recommend.userbase.peoplerank;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;

public class Step3PeopleRank_PageRankJob {

    public static final String HDFS = "hdfs://192.168.201.11:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void run(Map<String, String> path) {

        try {
            Step3PeopleRank_AdjacencyMatrix.run(path);
            int iter = Recommend.iterN;
            for (int i = 0; i < iter; i++) {// 迭代执行
                Step3PeopleRank_PageRank.run(path,i);
            }
            Step3PeopleRank_Normal.run(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
   //     System.exit(0);
    }



    public static JobConf config() {// Hadoop集群的远程配置信息
        JobConf conf = new JobConf(Step3PeopleRank_PageRankJob.class);
        conf.setJobName("PageRank");
        conf.addResource("resources/hadoop/core-site.xml");
        conf.addResource("resources/hadoop/hdfs-site.xml");
       // conf.addResource("classpath:/hadoop/core-site.xml");
      //  conf.addResource("classpath:/hadoop/hdfs-site.xml");
      //  conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }

    public static String scaleFloat(float f) {// 保留6位小数
        DecimalFormat df = new DecimalFormat("##0.000000");
        return df.format(f);
    }

}
