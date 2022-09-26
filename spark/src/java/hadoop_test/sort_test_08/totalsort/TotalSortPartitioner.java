package hadoop_test.sort_test_08.totalsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * 为什么有不用默认分区？因为默认是key 的hashcode分区，这达不到全排序效果，所以需要自定义分区
 * 此外，回去复习正则的知识
 * @author ysq
 *
 */

public class TotalSortPartitioner extends Partitioner<IntWritable,IntWritable> {

	@Override
	public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
//		key.toString().matches("[0-9]")匹配10一下数子key.toString().matches("[0-9][0-9]匹配0-100
		if(key.toString().matches("[0-9]")|key.toString().matches("[0-9][0-9]")){
			return 0;
		}
		if(key.toString().matches("[0-9][0-9][0-9]")){
			return 1;
		}else{
			return 2;
		}
		
	}

}
