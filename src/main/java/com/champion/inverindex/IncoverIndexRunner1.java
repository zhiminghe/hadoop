package com.champion.inverindex;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.champion.join.JoinRunner;

public class IncoverIndexRunner1 {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(JoinRunner.class);
		job.setMapperClass(InverIndexMapper.class);
		job.setReducerClass(InverIndexReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, "C:/e/work/hadoop/day09/inver/input");
		String path = "C:/e/work/hadoop/day09/inver/output/";
		File f = new File(path);
		if(f.exists()){
			f.delete();
		}
		FileOutputFormat.setOutputPath(job, new Path(path));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	static class InverIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			FileSplit fs = (FileSplit)context.getInputSplit();
			for (String string : split) {
				context.write(new Text(string + "--" + fs.getPath().getName()), new IntWritable(1));
			}
		}
	}
	
	static class InverIndexReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value,Context context) throws IOException, InterruptedException {
			int i = 0;
			for (IntWritable intWritable : value) {
				i++;
			}
			context.write(key, new IntWritable(i));
		}
	}

}
