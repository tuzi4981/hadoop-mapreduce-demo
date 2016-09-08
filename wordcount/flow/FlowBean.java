package cn.itheima.bigdata.hadoop.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * ��hadoop�У��Զ����bean�����Ҫ�������紫�䣬����ʵ��hadoop�����л���ܣ�����ʵ�ֽӿ�Writable
 * hadoop�Դ������л��������Serializable��˵���������Ϣ���Ӿ���
 * ��hadoop��Ŀ�Ĺ滮�У��Ժ�����л����ƿ��ܲ���Avro��ܣ����Կ����ԣ�
 * @author duanhaitao@itcast.cn
 *
 */

// WritableComparable<T> extends Writable, Comparable<T> 
//���������ǵ��Զ���bean���治���Լ�д��implements Writable,Comparable<T>,�ᱨ�쳣
public class FlowBean implements WritableComparable<FlowBean>{
	
	private String phoneNbr;
	private long up_flow;
	private long d_flow;
	private long sum_flow;
	
	
	//��Ϊ���bean�ڷ����л�ʱ��Ҫ�������ʵ��������Ҫһ���޲ι��캯��
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
	 * �������е���Ϣ���л�д�������
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(phoneNbr);
		out.writeLong(up_flow);
		out.writeLong(d_flow);
		out.writeLong(sum_flow);
		
	}


	/**
	 * ���������з����л��������ֶΣ�����˳��Ҫ�����л�ʱд���˳��һ��
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
