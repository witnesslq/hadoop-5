package cn.xsx.hadoop.mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

	/**
	 * 框架每传递一组数据<phoneNB,{u,d,s}>,就调用一次reduce方法
	 * reduce中的业务逻辑就是遍历values，进行累加求和再输出
	 */
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		long up_flow_counter = 0;
		long down_flow_counter = 0;
		
		for(FlowBean flow:values){
			up_flow_counter+=flow.getUp_flow();
			down_flow_counter+=flow.getDown_flow();
		}
		
		context.write(key, new FlowBean(key.toString(),up_flow_counter,down_flow_counter));
	
	}

}
