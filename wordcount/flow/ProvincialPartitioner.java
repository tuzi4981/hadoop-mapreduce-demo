package cn.itheima.bigdata.hadoop.flow;

import java.util.HashMap;

import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincialPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE>{
	private static HashMap<String, Integer> areaMap = new HashMap<String, Integer>();
	
	//为了性能考虑，不能频繁查询外部数据库，而应该在任务启动之初一次性加载到内存中，然后广播给各个节点
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
