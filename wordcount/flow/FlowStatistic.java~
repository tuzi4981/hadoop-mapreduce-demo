package cn.itheima.bigdata.hadoop.flow;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 对流量日志进行按用户汇总统计
 * 如果要将结果按照手机号所属省份分文件输出，就要自定义一个partitioner，然后还要控制reduce task数量
 * @author duanhaitao@itcast.cn
 *
 */
public class FlowStatistic {

	
	public static class FlowStatisticMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {

			//自定义一个计数器用来记录不合规的输入数据行数
			Counter lineErrCounter = context.getCounter("Malformed", "MalformedLine");
			
			//拿到一行的内容
			String line = value.toString();
			
			//切分出各个字段
			String[] fields = StringUtils.split(line,"\t");
			try{
			//取出上行流量和下行流量
			long up_flow = Long.parseLong(fields[fields.length-3]);
			long d_flow = Long.parseLong(fields[fields.length-2]);
			FlowBean bean = new FlowBean(up_flow,d_flow);
			
			//取出手机号
			String phoneNbr = fields[1];
			//为bean加入手机号值，以免序列化时出现空指针
			bean.setPhoneNbr(phoneNbr);
			
			context.write(new Text(phoneNbr),bean);
			}catch(Exception e){
				e.printStackTrace();
				lineErrCounter.increment(1);
				System.out.println("Exception occured in the mapper..........");
			}
		}
	}
	
	
	public static class FlowStatisticReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		
		
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values,Context context)
				throws IOException, InterruptedException {

			long up_sum = 0;
			long d_sum = 0;
			for(FlowBean bean: values){
				up_sum += bean.getUp_flow();
				d_sum += bean.getD_flow();
			}
		
			FlowBean bean = new FlowBean(up_sum, d_sum);
			
			context.write(key, bean);
		
		}
		
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		Job flowStatisticJob = Job.getInstance(conf);
		
		flowStatisticJob.setJarByClass(FlowStatistic.class);
		
		flowStatisticJob.setMapperClass(FlowStatisticMapper.class);
		flowStatisticJob.setReducerClass(FlowStatisticReducer.class);
		
		flowStatisticJob.setOutputKeyClass(Text.class);
		flowStatisticJob.setOutputValueClass(FlowBean.class);
		
		
		//指定reduce task的数量
		//partition的个数应该与reducetask数量保持一致
		//如果  reducetask数量 > partition ,则会产生多余的空结果文件
		//如果  reducetask数量 < partition ,则会抛出异常
		//如果  reducetask数量 < partition  &&   reducetask数量=1,也能正常运行，但是所有的kv都到了这一个reducer里面
		
		flowStatisticJob.setNumReduceTasks(1);
		//指定shuffle时使用partitioner类
		flowStatisticJob.setPartitionerClass(ProvincialPartitioner.class);
		
		
		FileInputFormat.setInputPaths(flowStatisticJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(flowStatisticJob, new Path(args[1]));
		
		
		boolean res = flowStatisticJob.waitForCompletion(true);
		System.exit(res?0:1);
		
	}
	
}
