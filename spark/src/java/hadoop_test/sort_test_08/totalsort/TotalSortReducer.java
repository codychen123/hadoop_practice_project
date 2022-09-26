package hadoop_test.sort_test_08.totalsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class TotalSortReducer extends Reducer<IntWritable,IntWritable ,IntWritable, IntWritable> {
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,
                          Context context)
			throws IOException, InterruptedException {
		int result=0;
		for(IntWritable value:values){
			result=result+value.get();
		}
		context.write(key, new IntWritable(result));
	}
}
