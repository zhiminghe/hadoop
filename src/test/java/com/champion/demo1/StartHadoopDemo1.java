package com.champion.demo1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class StartHadoopDemo1 {
	
	private FileSystem fs;
	@Before
	public void init() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://suzhou:9000");
		fs = FileSystem.get(conf);
	}
	
	@Test
	public void testCenntion() throws Exception {
		FileStatus[] fileStatus = fs.listStatus(new Path("/data/totay/"));
		System.out.println("###############################################");
		for (FileStatus status : fileStatus) {
			System.out.println(status);
		}
		System.out.println("###############################################");
	}
	
	
}
