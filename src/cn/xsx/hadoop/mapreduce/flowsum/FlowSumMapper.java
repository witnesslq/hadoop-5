package cn.xsx.hadoop.mapreduce.flowsum;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * FlowBean为自定义的一种数据类型，需要在hadoop中的各个节点之间传输，需要遵循hadoop的序列化机制，
 * 必须实现hadoop相应的序列化接口Writable
 * 
 * @author xusha
 *
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//循环--拿到日志中的一行数据，切分字段，抽取需要的字段
		
		String line = value.toString();//拿一行数据
		String[] fields = StringUtils.split(line,"\t");//切分数据
		
		//抽取需要的字段
		String phoneNB = fields[1];
		long u_flow = Long.parseLong(fields[7]);
		long d_flow = Long.parseLong(fields[8]);
		
		//封装数据key-value
		context.write(new Text(phoneNB), new FlowBean(phoneNB, u_flow, d_flow));
	}
	
}
