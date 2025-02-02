package com.paradeto.recommend;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * 得到用户评分向量
 *
 */
public class Step1{

    public static class Step1_ToItemPreMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final static IntWritable k = new IntWritable();
        private final static Text v = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(value.toString());
            int userID = Integer.parseInt(tokens[0]);
            String itemID = tokens[1];
            String pref = tokens[2];
            k.set(userID);
            v.set(itemID + ":" + pref);
            context.write(k, v);
        }
    }

    public static class Step1_ToUserVectorReducer extends Reducer <IntWritable, Text, IntWritable, Text> {
        private final static Text v = new Text();
        
        @Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, IntWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
        	 StringBuilder sb = new StringBuilder();
             for (Text value:values) {
                 sb.append("," + value.toString());
             }
             v.set(sb.toString().replaceFirst(",", ""));
             context.write(key, v);
		}

    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
    	//获得配置信息
    	Configuration conf = Recommend.config();
    	//得到输入输出路径
        Path input = new Path(path.get("Step1Input")+"/small2.csv");
        Path output = new Path(path.get("Step1Output"));
        //将本地文件上传到集群
        HDFSFile hdfs = new HDFSFile(new Path(Recommend.HDFS));
        hdfs.delFile(new Path(path.get("Step1Input")));
        hdfs.mkDir(new Path(path.get("Step1Input")));
        hdfs.copyLocalToHdfs(new Path(path.get("data")), new Path(path.get("Step1Input")));
        hdfs.copyLocalToHdfs(new Path(path.get("data2")), new Path(path.get("Step1Input")));
        //设置作业参数
        Job job = new Job(conf,"Step1");
		job.setJarByClass(Step1.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.setCompressOutput(job, true);
        TextOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);
        //job.setNumMapTasks(2); // 设置 Mapper 数量为 2
        job.setNumReduceTasks(1); // 设置 Reducer 数量为 1
        conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728"); // 128MB
        conf.set("mapreduce.input.fileinputformat.split.maxsize", Long.valueOf(536870912L *4).toString());
		
		job.setMapperClass(Step1_ToItemPreMapper.class);
		job.setCombinerClass(Step1_ToUserVectorReducer.class);
		job.setReducerClass(Step1_ToUserVectorReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,input);
		FileOutputFormat.setOutputPath(job,output);
		//运行作业
		job.waitForCompletion(true);
    }

}
