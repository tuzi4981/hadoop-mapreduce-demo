package cn.itheima.bigdata.hadoop.flow.sort;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.itheima.bigdata.hadoop.flow.FlowBean;

public class FlowSort {

	public static class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable>{
		
		private FlowBean bean = new FlowBean();
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = StringUtils.split(line,"\t");
			String phoneNbr = fields[0];
			long up_flow = Long.parseLong(fields[1]);
			long d_flow = Long.parseLong(fields[2]);
			bean.set(phoneNbr, up_flow, d_flow);
			context.write(bean,NullWritable.get());
			
		}
		
	}
	
	public static class FlowSortReducer extends Reducer<FlowBean, NullWritable, Text, FlowBean>{
		
		@Override
		protected void reduce(FlowBean bean, Iterable<NullWritable> values,Context context)
				throws IOException, InterruptedException {
			context.write(new Text(bean.getPhoneNbr()),bean);
		}
		
		
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowSort.class);
		
		job.setMapperClass(FlowSortMapper.class);
		job.setReducerClass(FlowSortReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
	
	
}
