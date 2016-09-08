package cn.itheima.bigdata.hadoop.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 在hadoop中，自定义的bean如果需要进行网络传输，必须实现hadoop的序列化框架，就是实现接口Writable
 * hadoop自带的序列化机制相比Serializable来说，传输的信息更加精简
 * 在hadoop项目的规划中，以后的序列化机制可能采用Avro框架（可以跨语言）
 * @author duanhaitao@itcast.cn
 *
 */

// WritableComparable<T> extends Writable, Comparable<T> 
//但是在我们的自定义bean里面不能自己写成implements Writable,Comparable<T>,会报异常
public class FlowBean implements WritableComparable<FlowBean>{
	
	private String phoneNbr;
	private long up_flow;
	private long d_flow;
	private long sum_flow;
	
	
	//因为这个bean在反序列化时需要被反射出实例，就需要一个无参构造函数
	public FlowBean(){}
	
	public FlowBean(long up_flow,long d_flow){
		this.up_flow = up_flow;
		this.d_flow = d_flow;
		this.sum_flow = up_flow + d_flow;
	}
	
	
	public void set(String phoneNbr,long up_flow,long d_flow){
		this.phoneNbr = phoneNbr;
		this.up_flow = up_flow;
		this.d_flow = d_flow;
		this.sum_flow = up_flow + d_flow;
	}
	
	
	
	public long getUp_flow() {
		return up_flow;
	}
	public void setUp_flow(long up_flow) {
		this.up_flow = up_flow;
	}
	public long getD_flow() {
		return d_flow;
	}
	public void setD_flow(long d_flow) {
		this.d_flow = d_flow;
	}
	public long getSum_flow() {
		return sum_flow;
	}
	public void setSum_flow(long sum_flow) {
		this.sum_flow = sum_flow;
	}

	
	public String getPhoneNbr() {
		return phoneNbr;
	}

	public void setPhoneNbr(String phoneNbr) {
		this.phoneNbr = phoneNbr;
	}

	/**
	 * 将对象中的信息序列化写入输出流
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(phoneNbr);
		out.writeLong(up_flow);
		out.writeLong(d_flow);
		out.writeLong(sum_flow);
		
	}


	/**
	 * 从数据流中反序列化出各个字段，读的顺序要与序列化时写入的顺序一致
	 */
	@Override
	public void readFields(DataInput in) throws IOException {

		phoneNbr = in.readUTF();
		up_flow = in.readLong();
		d_flow = in.readLong();
		sum_flow = in.readLong();
		
	}
	
	@Override
	public String toString() {
		 
		return up_flow + "\t" + d_flow + "\t" + sum_flow;
	}

	@Override
	public int compareTo(FlowBean o) {
		
		return this.sum_flow > o.getSum_flow()?-1:1;
	}
	
	
}
