package cn.itheima.bigdata.hadoop.flow;

import java.util.HashMap;

import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincialPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE>{
	private static HashMap<String, Integer> areaMap = new HashMap<String, Integer>();
	
	//Ϊ�����ܿ��ǣ�����Ƶ����ѯ�ⲿ���ݿ⣬��Ӧ������������֮��һ���Լ��ص��ڴ��У�Ȼ��㲥�������ڵ�
	static{
		areaMap.put("135", 0);
		areaMap.put("136", 1);
		areaMap.put("137", 2);
		areaMap.put("139", 3);
		areaMap.put("159", 4);
	}
	
	
	@Override
	public int getPartition(KEY key, VALUE value, int numPartitions) {
		
		Integer provinceCode = areaMap.get(key.toString().substring(0, 3));
		
		return provinceCode==null?5:provinceCode;
	}

}
