package com.champion.inverindex;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.champion.join.JoinRunner;

public class IncoverIndexRunner2 {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(JoinRunner.class);
		job.setMapperClass(InverIndexMapper.class);
		job.setReducerClass(InverIndexReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, "C:/e/work/hadoop/day09/inver/input1");
		String path = "C:/e/work/hadoop/day09/inver/output1/";
		File f = new File(path);
		if(f.exists()){
			f.delete();
		}
		FileOutputFormat.setOutputPath(job, new Path(path));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	static class InverIndexMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			String[] split2 = split[0].split("--");
			context.write(new Text(split2[0]), new Text(split2[1] + "--" + split[1]));
		}
	}
	
	static class InverIndexReduce extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> value,Context context) throws IOException, InterruptedException {
			String s = "";
			for (Text text : value) {
				s += text +"\t";
			}
			context.write(key, new Text(s));
		}
	}

}
