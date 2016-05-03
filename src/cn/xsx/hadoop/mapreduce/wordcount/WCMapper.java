package cn.xsx.hadoop.mapreduce.wordcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 4个泛型中，前两个是指定mapper输入数据的类型，KEYIN是输入的key的类型，VALUEIN是输入的value的类型
 * map 和 reduce的数据输入输出都是以key-value对的形式封装的
 * 默认情况下框架传递给我们的mapper的输入数据中，key是要处理的文本中的一行的起始偏移量，这一行的内容作为value
 * @author xusha
 *
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	/*
	 * mapreduce框架每读一行数据就要调用该方法一次
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//具体业务逻辑代码
		
		String line = value.toString();//将一行内容转换成String类型
		//文本内容拆分
		String[] words = StringUtils.split(line, " ");
		for(String word:words){//遍历
			context.write(new Text(word), new LongWritable(1));
		}
		
		
	}
	
}
