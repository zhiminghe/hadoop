package com.champion.countflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import lombok.Data;

@Data
public class FlowBean implements Writable{

	private Long uploadFlow;
	private Long downloadFlow;
	
	public Long getSumFlow() {
		return (uploadFlow == null ? 0:uploadFlow) + (downloadFlow == null ? 0:downloadFlow);
	}
	
	public void readFields(DataInput in) throws IOException {
		uploadFlow = in.readLong();
		downloadFlow = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(uploadFlow == null ? 0 : uploadFlow);
		out.writeLong(downloadFlow == null ? 0 : downloadFlow);
	}

}
