package hadoop_test.sort_test_08.domain;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Movie implements WritableComparable<Movie> {
	
	private String name;
	private int hot;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(hot);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.name=in.readUTF();
		this.hot=in.readInt();
		
	}

	@Override
	public int compareTo(Movie o) {
		
		return o.hot-this.hot;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getHot() {
		return hot;
	}

	public void setHot(int hot) {
		this.hot = hot;
	}

	@Override
	public String toString() {
		return "Movie [name=" + name + ", hot=" + hot + "]";
	}
	
	

}
