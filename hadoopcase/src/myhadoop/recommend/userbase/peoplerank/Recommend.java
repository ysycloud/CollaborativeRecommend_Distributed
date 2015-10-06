package myhadoop.recommend.userbase.peoplerank;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;
import myhadoop.hdfs.HdfsDAO;

//目前打jar的问题：（1）setjarclass；（2）提交测试集和数据集等的文件
public class Recommend {

    public static final String HDFS = "hdfs://192.168.201.11:9000";
    public static final Pattern DELIMITER = Pattern.compile("\t|,|::");
    
    public static int DataPool = 943;  //数据规模，基于项目相似度就是项目数，基于用户相似度就是用户数
    
    public static int KNEIGHBOUR = DataPool/2;	//number of neighbors最近邻个数（这个比项目数还多就相当于没设）
    public static float THRESHOLD = 0f ; //3f;	//与其他项目平均相似度的最低值，低于剔除
    public static int FilterWay = 1 ; //过滤策略：1、局部每项；2、过滤平均相似度的列
    
    public static int TestRECOMMENDER_NUM = 5; //测试集推荐数，查全率分母
    public static int ResultRECOMMENDER_NUM = 5; //计算结果集推荐数，查准率分母
    
    public static float RelationTHRESHOLD = 0.85f ; //相似度大于它算作好友，相互关注
    public static int iterN = 10; //PeopleRank总迭代次数
    

    public enum Correlation {
    	Conoccurrence , Cos , Pearson , Euclid ;       
    } 
        
    public static Correlation c = Correlation.Pearson;  //定义相似度参数
   
    
    public static void main(String[] args) throws Exception {
    	 	
    	int part = 1;  //执行spark前的部分[1]还是后的部分[2],或不执行spark[3]
    	if(args.length==8){
    		DataPool = Integer.parseInt(args[0]);
    		KNEIGHBOUR = DataPool/2; 
    		THRESHOLD = Float.parseFloat(args[1]);
    		if(Integer.parseInt(args[2])!=1&&Integer.parseInt(args[2])!=2){
    			System.out.println("args3(FilterWay) error！");
	    		System.exit(0);
    		}    			
    		FilterWay = Integer.parseInt(args[2]); 		
    		TestRECOMMENDER_NUM = Integer.parseInt(args[3]);
    		ResultRECOMMENDER_NUM = Integer.parseInt(args[4]);   		
    		switch(Integer.parseInt(args[5])){
    			case 1:
    				c = Correlation.Conoccurrence;
    				break;
    			case 2:
    				c = Correlation.Cos;
    				break;
    			case 3:
    				c = Correlation.Pearson;
    				break;
    			case 4:
    				c = Correlation.Euclid;
    				break;
    			default:
    				System.out.println("args7(Correlation) error！");
    	    		System.exit(0);
    		}
    		RelationTHRESHOLD = Float.parseFloat(args[6]);
    		if(Integer.parseInt(args[7])!=1&&Integer.parseInt(args[7])!=2&&Integer.parseInt(args[7])!=3){
    			System.out.println("args8(part) error！");
	    		System.exit(0);
    		}    			
    		part = Integer.parseInt(args[7]);
    	}else if(args.length != 0){
    		System.out.println("args error！");
    		System.exit(0);    		
    	}
    	//没有参数就按默认参数执行
    	   	
        Map<String, String> path = new HashMap<String, String>();
        path.put("data", "logfile/u.base");
        path.put("vertices", "logfile/graphx-vertices.txt"); //用spark必要时传入节点信息
        path.put("Step1Input", HDFS + "/user/hdfs/recommend/userbase");
        path.put("Step1Output", path.get("Step1Input") + "/step1");
        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", path.get("Step1Input") + "/step2");
        
        path.put("Step3Input1", path.get("Step1Output"));
        path.put("Step3Output1", path.get("Step1Input") + "/step3_1");
        path.put("Step3Input2", path.get("Step2Output"));
        path.put("Step3Output2", path.get("Step1Input") + "/step3_2");
        path.put("Step3Input3", path.get("Step2Output"));
        path.put("Step3Output3", path.get("Step1Input") + "/step3_3"); //peoplerank
        
        path.put("Step3SparkInput3", path.get("Step2Output")); //用spark 进行peoplerank需要的预处理的only-map
        path.put("Step3SparkOutput3", path.get("Step1Input") + "/step3_3_spark"); 
        
        /*
         * *******************peoplerank的path***************************
         */
        path.put("nums", "943");// 用户数
        path.put("d", "0.85");// 阻尼系数

        path.put("input_pr", HDFS + "/user/hdfs/recommend/pagerank/pr");// pr存储目
        path.put("tmp1", HDFS + "/user/hdfs/recommend/pagerank/tmp1");// 临时目录,存放邻接矩阵
        path.put("tmp2", HDFS + "/user/hdfs/recommend/pagerank/tmp2");// 临时目录,计算到得PR,覆盖input_pr        
        /****************************************************************************/
       
        
        path.put("Step4Input1", path.get("Step3Output1"));
        path.put("Step4Input2", path.get("Step3Output2"));
        path.put("Step4Input3", path.get("Step3Output3"));
        path.put("Step4Output", path.get("Step1Input") + "/step4");
        
        path.put("Step5Input", path.get("Step4Output"));
        path.put("Step5Output", path.get("Step1Input") + "/step5");
        
        //移除结果集中已经看过的项目
        path.put("RemoveInput1", path.get("Step1Input")+"/removesource");
        path.put("RemoveInput2", path.get("Step5Output"));
        path.put("RemoveOutput", path.get("Step1Input") + "/remove"); 

        path.put("test", "logfile/u.test");
        //只保留结果集中测试集中有的项目
        path.put("KeepInput1", path.get("Step1Input")+"/test");
        path.put("KeepInput2", path.get("Step5Output"));
        path.put("KeepOutput", path.get("Step1Input") + "/keep");
        
        //测试误差
        path.put("TestInput1", path.get("Step1Input")+"/test");//测试数据集路径
        path.put("TestInput2", path.get("RemoveOutput"));
        path.put("TestOutput", path.get("Step1Input") + "/testresult");
        
        //获取推荐结果
        path.put("RecallPresionStep1Input1", path.get("Step1Input")+"/test"); //测试数据集路径
        path.put("RecallPresionStep1Output1", path.get("Step1Input")+"/testrecommend"); //测试集的每个人的推荐列表
        path.put("RecallPresionStep1Input2_1", path.get("RemoveOutput"));  //去已看后的计算数据集
        path.put("RecallPresionStep1Input2_2", path.get("KeepOutput"));  //只保留测试项的计算数据集
        path.put("RecallPresionStep1Output2", path.get("Step1Input")+"/resultrecommend"); //结果集的每个人的推荐列表
        
        //计算查全率和查准率
        path.put("RecallPresionStep2Input1", path.get("RecallPresionStep1Output1")); 
        path.put("RecallPresionStep2Input2", path.get("RecallPresionStep1Output2"));  
        path.put("RecallPresionStep2Output", path.get("Step1Input")+"/recallpresion"); //测试集的每个人的推荐列表
        
        if(part==1){  //spark前
        	Step1.run(path);      
            switch(c){    //求项目两两之间的相似度/同现值，比较耗时
            	case Conoccurrence:
            		Step2.run(path);
            		break;
            	/*
                * 非同现值的相似度在计算时尤其地快，可能是step3选出的那些平均相似度较大的item列下元素比较少
                */
            	case Cos:
            		Step2_CosCorrelation.run(path);
            		break;
            	case Pearson:
            		Step2_PearsonCorrelation.run(path);
            		break;
            	case Euclid:
            		Step2_EuclidDistanceCorrelation.run(path);
            		break;
            }        
            Step3.run1(path);  //only-map评分矩阵      
            if(FilterWay==1)
            	Step3_Filter1.run(path);
            else
            	Step3_Filter2.run(path);           
             //  Step3PeopleRank_PageRankJob.run(path); //求PeopleRank(MapReduce),这两者选其一
            Step3PeopleRank_SparkPreprocess.run(path); //用spark求PeopleRank的预处理
        }else if(part == 2){ //spark后
        	Step4_Update.run(path);     //当阈值比较小，近邻比较多时（剔除的列少），Step4的这两步比较耗时（可能写出近1G的中间数据[最小数据集下]）
            Step4_Update2.run(path);   //这一步map完了会shuffle挺久
            
//            RemoveAlreadyRecommend.run(path);   //去除结果中已看过的项目
//            KeepDataInTest.run(path);    //只保留计算结果中测试集里有的数据
//            
//            TestMAERecomend.run(path);    //测试评分值的误差（测试集和结果集都有的那些数据项）
//                   
//            RecallPresionTestStep1.run1(path);	//得到测试集的推荐结果
//            RecallPresionTestStep1.run2(path,1);    //得到结果集的推荐结果       
//            RecallPresionTestStep2.run(path);	//计算查全率和查准率
        }else{  //不用spark
        	Step1.run(path);      
            switch(c){    //求项目两两之间的相似度/同现值，比较耗时
            	case Conoccurrence:
            		Step2.run(path);
            		break;
            	/*
                * 非同现值的相似度在计算时尤其地快，可能是step3选出的那些平均相似度较大的item列下元素比较少
                */
            	case Cos:
            		Step2_CosCorrelation.run(path);
            		break;
            	case Pearson:
            		Step2_PearsonCorrelation.run(path);
            		break;
            	case Euclid:
            		Step2_EuclidDistanceCorrelation.run(path);
            		break;
            }        
            Step3.run1(path);  //only-map评分矩阵      
            if(FilterWay==1)
            	Step3_Filter1.run(path);
            else
            	Step3_Filter2.run(path);           
            Step3PeopleRank_PageRankJob.run(path); //求PeopleRank(MapReduce),这两者选其一
            //Step3PeopleRank_SparkPreprocess.run(path); //用spark求PeopleRank的预处理
            Step4_Update.run(path);     //当阈值比较小，近邻比较多时（剔除的列少），Step4的这两步比较耗时（可能写出近1G的中间数据[最小数据集下]）
            Step4_Update2.run(path);   //这一步map完了会shuffle挺久
            
//            RemoveAlreadyRecommend.run(path);   //去除结果中已看过的项目
//            KeepDataInTest.run(path);    //只保留计算结果中测试集里有的数据
//            
//            TestMAERecomend.run(path);    //测试评分值的误差（测试集和结果集都有的那些数据项）
//                   
//            RecallPresionTestStep1.run1(path);	//得到测试集的推荐结果
//            RecallPresionTestStep1.run2(path,1);    //得到结果集的推荐结果       
//            RecallPresionTestStep2.run(path);	//计算查全率和查准率
        }
        
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
