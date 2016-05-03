package cn.xsx.hadoop.mapreduce.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WCRunner {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(WCRunner.class);//设置job所用的那些类所在的位置
		
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		
		//指定reducer的输出数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//指定mapper的输出数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//指定原始数据源
		FileInputFormat.setInputPaths(job, new Path("/wc/insrc"));
		
		FileOutputFormat.setOutputPath(job, new Path("/wc/output"));
		
		job.waitForCompletion(true);//将job提交给集群运行
	}
}	
