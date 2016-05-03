package cn.xsx.hadoop.mapreduce.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.jasper.tagplugins.jstl.core.Out;

public class FlowBean implements Writable {
	
	private String phoneNB;
	private long up_flow;
	private long down_flow;
	private long sum_flow;
	
	
	public FlowBean() {//反序列化时，反射机制需要调用空参构造函数，所以必须显示定义空构造函数
	}

	/**
	 * 定义一个带参构造函数，方便对象数据的初始化
	 */
	public FlowBean(String phoneNB, long up_flow, long down_flow) {
		this.phoneNB = phoneNB;
		this.up_flow = up_flow;
		this.down_flow = down_flow;
		this.sum_flow = up_flow+down_flow;
	}

	public String getPhoneNB() {
		return phoneNB;
	}

	public void setPhoneNB(String phoneNB) {
		this.phoneNB = phoneNB;
	}

	public long getUp_flow() {
		return up_flow;
	}

	public void setUp_flow(long up_flow) {
		this.up_flow = up_flow;
	}

	public long getDown_flow() {
		return down_flow;
	}

	public void setDown_flow(long down_flow) {
		this.down_flow = down_flow;
	}

	public long getSum_flow() {
		return sum_flow;
	}

	public void setSum_flow(long sum_flow) {
		this.sum_flow = sum_flow;
	}

	/**
	 * 从数据流中反序列化对象的数据
	 * 读数据的时候必须和序列化的顺序一致
	 */
	@Override
	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		phoneNB = input.readUTF();
		up_flow = input.readLong();
		down_flow = input.readLong();
		sum_flow = input.readLong();
	}
	
	
	/**
	 * 将对象序列化到流中
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(phoneNB);
		out.writeLong(up_flow);
		out.writeLong(down_flow);
		out.writeLong(sum_flow);
	}

	@Override
	public String toString() {
		return "up_flow=" + up_flow + ", down_flow=" + down_flow + ", sum_flow=" + sum_flow ;
	}
	
	

}
