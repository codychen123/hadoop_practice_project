package hadoop_test.min_max_demo_04;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxReducer extends Reducer<Text, IntWritable, Text, Text> {
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {

		int max=0;
		int min=Integer.MAX_VALUE;
		for(IntWritable value:values){
			if(max<value.get()){
				max=value.get();
			}
			if(min>value.get()){
				min=value.get();
			}
		}
		context.write(key, new Text(String.valueOf(max)+":"+String.valueOf(min)));
	}

}
