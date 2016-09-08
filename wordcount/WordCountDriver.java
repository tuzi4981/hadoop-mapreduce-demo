package cn.xiebo.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool{

	/**
	 * 在run中对job进行封装
	 */
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		//手动指定jar包的路径
		conf.set("mapreduce.job.jar", "wordcount.jar");
		
		//先构造一个用来提交业务程序封装的一个对象
		Job job = Job.getInstance(conf);
		//将业务程序所在jar的路径封装到job中
		job.setJarByClass(WordCountDriver.class);
		
		//设置mapper所在类
		job.setMapperClass(WordCountMapper.class);
		//设置reducer所在类
		job.setReducerClass(WordCountReducer.class);
		//指定本job所在的Combiner组件
		job.setCombinerClass(WordCountCombiner.class);
		//设置mapper类输出的kv数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		//设置reducer类输出的kv数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		//指定要处理的文件在hdfs的路径
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/wordcount/srcdata"));
		//指定输出结果所存路径
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/wordcount/result"));
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCountDriver(), args);
		System.exit(res);
	}
}
