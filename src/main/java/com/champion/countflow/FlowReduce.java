package com.champion.countflow;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean>{

	private FlowBean flow = new FlowBean();
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {
		Long uploadFlow = 0l;
		Long downLoadFlow = 0l;
		for (FlowBean flowBean : values) {
			uploadFlow += flowBean.getUploadFlow();
			downLoadFlow += flowBean.getDownloadFlow();
		}
		flow.setDownloadFlow(downLoadFlow);
		flow.setUploadFlow(uploadFlow);
		context.write(key, flow);
	}
	
}
