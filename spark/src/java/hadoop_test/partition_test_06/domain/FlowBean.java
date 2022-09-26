package hadoop_test.partition_test_06.domain;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//实现一个writable接口，并重写两个方法，write表示序列化,readFields表示反序列化
public class FlowBean implements Writable {
	
	private String phone;
	private String addr;
	private String name;
	private int consum;
	
	

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(phone);
		out.writeUTF(addr);
		out.writeUTF(name);
		out.writeInt(consum);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.phone=in.readUTF();
		this.addr=in.readUTF();
		this.name=in.readUTF();
		this.consum=in.readInt();
		
	}
	
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getAddr() {
		return addr;
	}
	public void setAddr(String addr) {
		this.addr = addr;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getFlow() {
		return consum;
	}
	public void setFlow(int flow) {
		this.consum = flow;
	}
	@Override
	public String toString() {
		return "FlowBean [phone=" + phone + ", addr=" + addr + ", name=" + name + ", consum=" + consum + "]";
	}
	
	

}
