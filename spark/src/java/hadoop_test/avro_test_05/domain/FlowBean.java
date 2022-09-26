package hadoop_test.avro_test_05.domain;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean  implements Writable{

	private String phone;
	private String add;
	private String name;
	private long consum;

//	序列化
	@Override
	public void write(DataOutput out) throws IOException {

		out.writeUTF(phone);
		out.writeUTF(add);
		out.writeUTF(name);
		out.writeLong(consum);

	}

//	反序列化,跟序列化的顺序不能改变
	@Override
	public void readFields(DataInput in) throws IOException {
		this.phone=in.readUTF();
		this.add=in.readUTF();
		this.name=in.readUTF();
		this.consum=in.readLong();
	}

	public String getPhone() {
		return phone;
	}

	public String getAdd() {
		return add;
	}

	public String getName() {
		return name;
	}

	public long getConsum() {
		return consum;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public void setAdd(String add) {
		this.add = add;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setConsum(long consum) {
		this.consum = consum;
	}

	@Override
	public String toString() {
		return "FlowBean [phobe="+phone+",add="+add+",name="+name+",consum="+consum+"]";
	}
}