package com.champion.countflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowcountRunner {
	
	public static void main(String[] args) throws Exception {
		String outPath = "hdfs://suzhou:9000/flow/output" + args[0];
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://suzhou:9000");
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowcountRunner.class);
		job.setMapperClass(FlowMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setReducerClass(FlowReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		FileInputFormat.setInputPaths(job, "hdfs://suzhou:9000/flow/input/flow.log");
		FileSystem fileSystem = FileSystem.get(conf);
		if(fileSystem.exists(new Path(outPath))) {
			fileSystem.delete(new Path(outPath),true);
		}
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		boolean rs = job.waitForCompletion(true);
		System.exit(rs ? 0 : 1);
		
	}
	
}
