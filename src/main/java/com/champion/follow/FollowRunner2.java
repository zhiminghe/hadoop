package com.champion.follow;

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

public class FollowRunner2 {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(JoinRunner.class);
		job.setMapperClass(FollowMapper1.class);
		job.setReducerClass(FollowReduce1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, "C:/e/work/hadoop/day09/follow/input");
		String path = "C:/e/work/hadoop/day09/follow/output/";
		File f = new File(path);
		if(f.exists()){
			f.delete();
		}
		FileOutputFormat.setOutputPath(job, new Path(path));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
	static class FollowMapper1 extends Mapper<LongWritable, Text, Text, Text> {
		
		Text outKey = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(":");
			String[] split2 = split[1].split(",");
			Text outValue = new Text(split[0]);
			for (String string : split2) {
				outKey.set(string);
				context.write(outKey, outValue);
			}
		}
	}
	
	static class FollowReduce1 extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> value,Context context) throws IOException, InterruptedException {
			StringBuffer br = new StringBuffer();
			for (Text text : value) {
				br.append(text.toString());
			}
			context.write(key, new Text(br.toString()));
		}
	}
}
