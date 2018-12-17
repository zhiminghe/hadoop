package com.champion.countflow;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
	
	private FlowBean flowBean = new FlowBean(); 
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String flows = value.toString();
		if(flows == null || "".equals(flows.trim())) {
			return;
		}
		String[] alltrimes = flows.split("\t");
		flowBean.setUploadFlow(Long.getLong(alltrimes[flows.length() - 3]));
		flowBean.setDownloadFlow(Long.getLong(alltrimes[flows.length() - 2]));
		context.write(new Text(alltrimes[1]), flowBean);
	}
	
}
