package com.paradeto.recommend;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import com.paradeto.recommend.Step1.Step1_ToItemPreMapper;
import com.paradeto.recommend.Step1.Step1_ToUserVectorReducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step3_corrMatrix {
    private static ArrayList<String> myList=new ArrayList<>();


    public static void addAnimeToList(HDFSFile hdfs,Map<String, String> path) throws IOException {
        //System.out.println("ArrayList中的元素:");
        //FS的输入流
        FileSystem tempFileSystem=hdfs.return_FileSystem();
        FSDataInputStream in = tempFileSystem.open(new Path(path.get("Step1Input")+"/small1.csv"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        // 逐行读取文件内容
        String line;
        while ((line = reader.readLine()) != null) {
            // 处理每一行数据
            myList.add(line.split(",")[0]);
        }
        //for(String str:myList)
            //System.out.println(str);
        // 关闭流
        reader.close();
    }
    public static class Step3_corrMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            Text v = new Text();
            Text k = new Text();
            k.set(tokens[0]);
            v.set(tokens[1]);
            context.write(k, v);
            //System.out.println("Map:"+k.toString()+"----"+v.toString());
            String itemID=tokens[0].split(":")[0];
            String itemID2=tokens[0].split(":")[1];
            if(itemID.equals(itemID2)){
                for(String itemID3:myList){
                    if(!itemID.equals(itemID3)){
                        k.set(itemID+":"+itemID3);
                        v.set(itemID+":"+tokens[1]);
                        context.write(k, v);
                        //System.out.println("Mapsjhfoijsdio:"+k.toString()+"----"+v.toString());
                        k.set(itemID3+":"+itemID);
                        v.set(itemID+":"+tokens[1]);
                        context.write(k, v);
                        //System.out.println("Mapsjhfoijsdio:"+k.toString()+"----"+v.toString());
                    }
                }
            }
        }
    }

    public static class Step3_corrMatrixReducer extends Reducer <Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {
            List<Text> list = new ArrayList<Text>();
            // 遍历迭代器，并将元素添加到列表中
            for(Text value:values) {
                list.add(new Text(value));
                //System.out.println(value.toString());
            }
            int num=0;
            for(Text value:list){
                //System.out.println("显示键值对::"+key.toString()+"----"+value.toString()+"--"+num);
                num=num+1;
            }
            //System.out.println("num值为"+num);
            if(num<2) {
                for(Text t:list)
                    result.set("1");

                //key.set(key);
            } else if (num<3) {
                result.set("0");

            } else{
                float denominator=1;
                float numerator=2;
                for(Text value:list){
                    String str=value.toString();
                    if(str.contains(":")){
                        denominator=denominator*((float) Math.sqrt(Float.parseFloat(str.split(":")[1])));
                    }
                    else{
                        numerator=Float.parseFloat(value.toString());
                    }
                }
                result.set(Float.toString(numerator/denominator));
                //System.out.println("计算协同方差");
            }
            context.write(key, result);
            //System.out.println("Reducesdfsdijf[sdpofp[:"+key.toString()+"----"+result.toString());
        }

    }
    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        //获得配置信息
        Configuration conf = Recommend.config();
        //得到输入输出路径
        Path input = new Path(path.get("Step3corrMatrixInput"));
        Path output = new Path(path.get("Step3CorrMatrixOutput"));
        //删掉上次输出结果
        HDFSFile hdfs = new HDFSFile(new Path(Recommend.HDFS));
        //读取所有动漫
        addAnimeToList(hdfs,path);
        //删掉上次输出结果
        hdfs.delFile(output);

        //设置作业参数
        Job job = new Job(conf);

        job.setJarByClass(Step3_corrMatrix.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setCompressOutput(job, true);
        TextOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.GzipCodec.class);
        //job.setNumMapTasks(2); // 设置 Mapper 数量为 2
        job.setNumReduceTasks(1); // 设置 Reducer 数量为 1
        conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728"); // 128MB
        conf.set("mapreduce.input.fileinputformat.split.maxsize", Long.valueOf(536870912L *4).toString());

        job.setMapperClass(Step3_corrMatrixMapper.class);

        //job.setCombinerClass(null);
        job.setReducerClass(Step3_corrMatrixReducer.class);


        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);


        FileInputFormat.addInputPath(job,input);

        FileOutputFormat.setOutputPath(job,output);

        //运行作业
        job.waitForCompletion(true);

    }

    public static void main(String[] args) {
        //System.out.println("hello");
    }
}
