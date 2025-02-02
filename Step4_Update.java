package com.paradeto.recommend;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 共现矩阵乘评分向量的乘法部分
 *
 */
public class Step4_Update {
	public static class Step4_PartialMultiplyMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String flag;// A同现矩阵/左矩阵 or B评分矩阵/右矩阵

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName();// 判断读的数据集
        }

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());

            if (flag.equals("step3_corrMatrix")) {// 同现矩阵
                String[] v1 = tokens[0].split(":");
                String itemID1 = v1[0];
                String itemID2 = v1[1];
                String num = tokens[1];

                Text k = new Text(itemID1);
                Text v = new Text("A:" + itemID2 + "," + num);

                context.write(k, v);
                //System.out.println("同现矩阵"+k.toString() + "  " + v.toString());

            } else if (flag.equals("step3_1")) {// 评分矩阵
                String[] v2 = tokens[1].split(":");
                String itemID = tokens[0];
                String userID = v2[0];
                String pref = v2[1];

                Text k = new Text(itemID);
                Text v = new Text("B:" + userID + "," + pref);

                context.write(k, v);
                //System.out.println("评分矩阵"+k.toString() + "  " + v.toString());
            }
        }

    }

    public static class Step4_AggregateReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //System.out.println(key.toString() + ":");

            Map<String, String> mapA = new HashMap<String, String>();
            Map<String, String> mapB = new HashMap<String, String>();

            for (Text line : values) {
                String val = line.toString();
                //System.out.println(val);

                if (val.startsWith("A:")) {
                    String[] kv = Recommend.DELIMITER.split(val.substring(2));
                    mapA.put(kv[0], kv[1]);

                } else if (val.startsWith("B:")) {
                    String[] kv = Recommend.DELIMITER.split(val.substring(2));
                    mapB.put(kv[0], kv[1]);

                }
            }
            //A:同现矩阵 B：评分矩阵
            double result = 0;
            Iterator<String> iter = mapA.keySet().iterator();
            while (iter.hasNext()) {
                String mapk = iter.next();// itemID

                double num = Double.parseDouble(mapA.get(mapk));
                Iterator<String> iterb = mapB.keySet().iterator();
                while (iterb.hasNext()) {
                    String mapkb = iterb.next();// userID
                    double pref = Double.parseDouble(mapB.get(mapkb));
                    result = num * pref;// 矩阵乘法相乘计算
                    
                    Text k = new Text(mapkb.toString());
                    Text v = new Text(mapk + "," + result);
              
                    context.write(k, v);
                    //System.out.println(k.toString() + "  " + v.toString());
                }
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
    	// 获得配置信息
		Configuration conf = Recommend.config();
		// 得到输入输出路径
		Path input1 = new Path(path.get("Step4_1Input1"));
		Path input2 = new Path(path.get("Step4_1Input2"));
		Path output = new Path(path.get("Step4_1Output"));
		// 删除上一次的输出
		HDFSFile hdfs = new HDFSFile(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// 设置作业参数
        Job job = new Job(conf);
        job.setJarByClass(Step4_Update.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setCompressOutput(job, true);
        TextOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);
        //job.setNumMapTasks(2); // 设置 Mapper 数量为 2
        job.setNumReduceTasks(1); // 设置 Reducer 数量为 1
        conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728"); // 128MB
        conf.set("mapreduce.input.fileinputformat.split.maxsize", Long.valueOf(536870912L *4).toString());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4_Update.Step4_PartialMultiplyMapper.class);
        job.setReducerClass(Step4_Update.Step4_AggregateReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input1, input2);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }
}
