package cn.xiebo.mapreduce;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper< LongWritable, Text, Text ,LongWritable >{
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		//拿到一行的内容,并切分成单词
		String[] words = value.toString().split(" ");
		//输出成<key,1>这种形式
		for(String word : words){
			context.write(new Text(word), new LongWritable(1));
		}
	}
}
