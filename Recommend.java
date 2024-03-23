package com.paradeto.recommend;

import java.io.BufferedReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import java.io.InputStreamReader;
import org.apache.hadoop.io.IOUtils;


import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IOUtils;
/**
 * 基于物品的协同推荐系统
 */
public class Recommend {
	//HDFS文件路径
	public static final String HDFS = "hdfs://127.0.0.1:9000";
	//MapReduce分割符为\t和,
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");
    //入口函数
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
    	Map<String, String> path = new HashMap<String, String>();
    	//本地数据路径
    	path.put("data", "D:\\works\\Git-Space\\Recommend\\code\\recommend\\data\\small2.csv");
        path.put("data2", "D:\\works\\Git-Space\\Recommend\\code\\recommend\\data\\small1.csv");
    	//步骤1的输入输出路径
        path.put("Step1Input", HDFS + "/recommend");
        path.put("Step1Output", path.get("Step1Input") + "/step1");
        //步骤2的输入输出路径
        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", path.get("Step1Input") + "/step2");
        //步骤3_1的输入输出路径
        path.put("Step3Input1", path.get("Step1Output"));
        path.put("Step3Output1", path.get("Step1Input") + "/step3_1");
        //步骤3_2的输入输出路径
        path.put("Step3Input2", path.get("Step2Output"));
        path.put("Step3Output2", path.get("Step1Input") + "/step3_2");
        //步骤3_corrMatrix的输入输出路径
        path.put("Step3corrMatrixInput", path.get("Step2Output"));
        path.put("Step3CorrMatrixOutput", path.get("Step1Input") + "/step3_corrMatrix");
        //步骤4的输入输出路径
        path.put("Step4_1Input1", path.get("Step3Output1"));
        path.put("Step4_1Input2", path.get("Step3CorrMatrixOutput"));
        path.put("Step4_1Output", path.get("Step1Input") + "/step4_1");      
        path.put("Step4_2Input", path.get("Step4_1Output"));
        path.put("Step4_2Output", path.get("Step1Input") + "/step4_2");
        //步骤5的输入输出路径
        path.put("Step5Input1", path.get("Step4_2Output"));
        path.put("Step5Input2", path.get("Step1Input")+"/small2.csv");
        path.put("Step5Output", path.get("Step1Input") + "/step5");
        

        Step1.run(path);
        Step2.run(path);
        Step3.run1(path);
        //Step3.run2(path);
        Step3_corrMatrix.run(path);
        Step4_Update.run(path);
        Step4_Update2.run(path);
        Step5.run(path);
        
        //输出结果到终端
        //HDFSFile hdfs = new HDFSFile(new Path(HDFS));
        //Step1的输出结果
        //System.out.println(path.get("Step1Output")+"/part-r-00000");
        //hdfs.readFile(new Path(path.get("Step1Output")+"/part-r-00000"));
        //Step2的输出结果
        //System.out.println(path.get("Step2Output")+"/part-r-00000");
        //hdfs.readFile(new Path(path.get("Step2Output")+"/part-r-00000"));
        //Step3_1的输出结果
        //System.out.println(path.get("Step3Output1")+"/part-r-00000");
        //hdfs.readFile(new Path(path.get("Step3Output1")+"/part-r-00000"));
        //Step3_2的输出结果
        //System.out.println(path.get("Step3Output2")+"/part-r-00000");
        //hdfs.readFile(new Path(path.get("Step3Output2")+"/part-r-00000"));
        //Step4_1的输出结果
        //System.out.println(path.get("Step4_1Output")+"/part-r-00000");
        //hdfs.readFile(new Path(path.get("Step4_1Output")+"/part-r-00000"));
        //System.exit(0);
        //Step4_2的输出结果
        //System.out.println(path.get("Step4_2Output")+"/part-r-00000");
        //hdfs.readFile(new Path(path.get("Step4_2Output")+"/part-r-00000"));
        //Step5的输出结果
        System.out.println(path.get("Step5Output")+"/part-r-00000");
        //hdfs.readFile(new Path(path.get("Step5Output")+"/part-r-00000"));
        String hdfsUri = "hdfs://127.0.0.1:9000";
        // HDFS上的gzip压缩文件路径
        String hdfsFilePath = "/recommend/step5/part-r-00000.gz";

        // 创建Hadoop配置对象
        Configuration conf = new Configuration();
        // 设置Hadoop集群配置，如果需要的话
        // conf.set("fs.defaultFS", hdfsUri);
        // ... 其他配置

        // 获取文件系统对象
        FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf);

        try (
                // 打开HDFS上的gzip文件
                GZIPInputStream gzipInputStream = new GZIPInputStream(fs.open(new Path(hdfsFilePath)));
                // 创建一个BufferedReader来读取文本内容
                BufferedReader reader = new BufferedReader(new InputStreamReader(gzipInputStream))
        ) { 
            String line;
            // 逐行读取gzip文件内容
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } finally {
            // 关闭文件系统
            IOUtils.closeStream(fs);
        }
    }
    public static Configuration config() {
        Configuration conf = new Configuration();
        return conf;
    }

}
