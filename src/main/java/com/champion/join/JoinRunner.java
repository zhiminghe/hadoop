package com.champion.join;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JoinRunner {
	
	/**
	 * 本地运行  需要本地编译的hadoop/bin
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(JoinRunner.class);
		job.setMapperClass(JoinMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, "C:/e/work/hadoop/day09/join/input");
		String path = "C:/e/work/hadoop/day09/join/output/";
		File f = new File(path);
		if(f.exists()){
			f.delete();
		}
		FileOutputFormat.setOutputPath(job, new Path(path));
		
		job.addCacheFile(new URI("file:/C:/e/work/hadoop/day09/join/product.txt"));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	static class JoinMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		Map<String, String> proInfos = new HashMap<String, String>();
		Text outKey = new Text();
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {		
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("C:/e/work/hadoop/day09/join/product.txt")));
			String line = null;
			while (StringUtils.isNotBlank(line = br.readLine())) {
				String[] split = line.split(",");
				proInfos.put(split[0], split[1]);
			}
			br.close();
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			String info = proInfos.get(split[2]);
			outKey.set(value.toString() + "\t" + info);
			context.write(outKey, NullWritable.get());
		}
		
	}
	
}
