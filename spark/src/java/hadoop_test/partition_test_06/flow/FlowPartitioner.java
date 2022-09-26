package hadoop_test.partition_test_06.flow;

import hadoop_test.partition_test_06.domain.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Random;

//Text,FlowBean  keymapput  keyvalueout
public class FlowPartitioner extends Partitioner<Text,FlowBean> {
	@Override
	//<Text,FlowBean>指的是map的key，value
	public int getPartition(Text key, FlowBean value, int numPartitions) {
//数据倾斜解决
//		Random R = new Random();
//
//		String hash_key=key.toString()+String.valueOf(R.nextInt());
//		return (hash_key .hashCode() & Integer.MAX_VALUE) % numPartitions;
//		if(value.getName().equals("wyf")){
//			Random R = new Random();
////
//			String hash_key = key.toString()+String.valueOf(R.nextInt());
//		}

		if(value.getAddr().equals("sh")){
			return 0;
		}
		if(value.getAddr().equals("bj")){
			return 1;
		}
		else{
			return 2;
		}

		
	}

}
