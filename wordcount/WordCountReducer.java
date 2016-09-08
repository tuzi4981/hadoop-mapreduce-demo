package cn.xiebo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	/**
	 * key:某一个单词
	 * values:这个单词的所有value,可以理解为{1,1,1,1,.....}
	 */
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long count = 0;
		for(LongWritable value: values){
			count+= value.get();
		}
		context.write(key, new LongWritable(count));
	}
}
