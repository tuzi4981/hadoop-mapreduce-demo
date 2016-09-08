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
 * ��������־���а��û�����ͳ��
 * ���Ҫ����������ֻ�������ʡ�ݷ��ļ��������Ҫ�Զ���һ��partitioner��Ȼ��Ҫ����reduce task����
 * @author duanhaitao@itcast.cn
 *
 */
public class FlowStatistic {

	
	public static class FlowStatisticMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {

			//�Զ���һ��������������¼���Ϲ��������������
			Counter lineErrCounter = context.getCounter("Malformed", "MalformedLine");
			
			//�õ�һ�е�����
			String line = value.toString();
			
			//�зֳ������ֶ�
			String[] fields = StringUtils.split(line,"\t");
			try{
			//ȡ��������������������
			long up_flow = Long.parseLong(fields[fields.length-3]);
			long d_flow = Long.parseLong(fields[fields.length-2]);
			FlowBean bean = new FlowBean(up_flow,d_flow);
			
			//ȡ���ֻ���
			String phoneNbr = fields[1];
			//Ϊbean�����ֻ���ֵ���������л�ʱ���ֿ�ָ��
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
		
		
		//ָ��reduce task������
		//partition�ĸ���Ӧ����reducetask��������һ��
		//���  reducetask���� > partition ,����������Ŀս���ļ�
		//���  reducetask���� < partition ,����׳��쳣
		//���  reducetask���� < partition  &&   reducetask����=1,Ҳ���������У��������е�kv��������һ��reducer����
		
		flowStatisticJob.setNumReduceTasks(1);
		//ָ��shuffleʱʹ��partitioner��
		flowStatisticJob.setPartitionerClass(ProvincialPartitioner.class);
		
		
		FileInputFormat.setInputPaths(flowStatisticJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(flowStatisticJob, new Path(args[1]));
		
		
		boolean res = flowStatisticJob.waitForCompletion(true);
		System.exit(res?0:1);
		
	}
	
}
